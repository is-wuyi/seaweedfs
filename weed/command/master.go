package command

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util/version"

	hashicorpRaft "github.com/hashicorp/raft"

	"slices"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/raft/protobuf"
	"github.com/spf13/viper"
	"google.golang.org/grpc/reflection"

	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"

	"github.com/seaweedfs/seaweedfs/weed/util/grace"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	weed_server "github.com/seaweedfs/seaweedfs/weed/server"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	m MasterOptions
)

const (
	raftJoinCheckDelay = 1500 * time.Millisecond // delay before checking if we should join a raft cluster
)

type MasterOptions struct {
	port                       *int
	portGrpc                   *int
	ip                         *string
	ipBind                     *string
	metaFolder                 *string
	peers                      *string
	mastersDeprecated          *string // deprecated, for backward compatibility in master.follower
	volumeSizeLimitMB          *uint
	volumePreallocate          *bool
	maxParallelVacuumPerServer *int
	// pulseSeconds       *int
	defaultReplication *string
	garbageThreshold   *float64
	whiteList          *string
	disableHttp        *bool
	metricsAddress     *string
	metricsIntervalSec *int
	raftResumeState    *bool
	metricsHttpPort    *int
	metricsHttpIp      *string
	heartbeatInterval  *time.Duration
	electionTimeout    *time.Duration
	raftHashicorp      *bool
	raftBootstrap      *bool
	telemetryUrl       *string
	telemetryEnabled   *bool
	debug              *bool
	debugPort          *int
	// shutdownCtx, when non-nil, tells startMaster to shut down once the ctx
	// is cancelled. Used by integration tests and by weed mini; nil for
	// standalone weed master.
	shutdownCtx context.Context
}

func init() {
	cmdMaster.Run = runMaster // break init cycle
	m.port = cmdMaster.Flag.Int("port", 9333, "http 监听端口")
	m.portGrpc = cmdMaster.Flag.Int("port.grpc", 0, "grpc 监听端口")
	m.ip = cmdMaster.Flag.String("ip", util.DetectedHostAddress(), "master <ip>|<server> 地址,同时用作标识符")
	m.ipBind = cmdMaster.Flag.String("ip.bind", "", "绑定的 IP 地址。为空则默认与 -ip 选项相同。")
	m.metaFolder = cmdMaster.Flag.String("mdir", os.TempDir(), "存储元数据的数据目录")
	m.peers = cmdMaster.Flag.String("peers", "", "以逗号分隔的 ip:port 列表形式给出所有 master 节点,例如:127.0.0.1:9093,127.0.0.1:9094,127.0.0.1:9095;单 master 模式时使用 'none'")
	m.volumeSizeLimitMB = cmdMaster.Flag.Uint("volumeSizeLimitMB", 30*1000, "master 停止向超限 volume 分配写入")
	m.volumePreallocate = cmdMaster.Flag.Bool("volumePreallocate", false, "为 volume 预分配磁盘空间。")
	m.maxParallelVacuumPerServer = cmdMaster.Flag.Int("maxParallelVacuumPerServer", 1, "每个 volume 服务器并行 vacuum 的最大 volume 数量")
	// m.pulseSeconds = cmdMaster.Flag.Int("pulseSeconds", 5, "心跳之间的秒数")
	m.defaultReplication = cmdMaster.Flag.String("defaultReplication", "", "未指定时的默认副本策略。")
	m.garbageThreshold = cmdMaster.Flag.Float64("garbageThreshold", 0.3, "vacuum 并回收空间的阈值")
	m.whiteList = cmdMaster.Flag.String("whiteList", "", "逗号分隔的具有写权限的 IP 地址列表,为空则不限制")
	m.disableHttp = cmdMaster.Flag.Bool("disableHttp", false, "禁用 http 请求,仅允许 gRPC 操作。")
	m.metricsAddress = cmdMaster.Flag.String("metrics.address", "", "Prometheus 网关地址 <host>:<port>")
	m.metricsIntervalSec = cmdMaster.Flag.Int("metrics.intervalSeconds", 15, "Prometheus 推送间隔,单位为秒")
	m.metricsHttpPort = cmdMaster.Flag.Int("metricsPort", 0, "Prometheus 指标监听端口")
	m.metricsHttpIp = cmdMaster.Flag.String("metricsIp", "", "指标监听 IP。为空则默认与 -ip.bind 选项相同。")
	m.raftResumeState = cmdMaster.Flag.Bool("resumeState", true, "启动 master 服务器时恢复先前的状态")
	m.heartbeatInterval = cmdMaster.Flag.Duration("heartbeatInterval", 300*time.Millisecond, "master 服务器的心跳间隔,将被随机乘以 [1, 1.25)")
	m.electionTimeout = cmdMaster.Flag.Duration("electionTimeout", 10*time.Second, "master 服务器的选举超时")
	m.raftHashicorp = cmdMaster.Flag.Bool("raftHashicorp", false, "使用 hashicorp raft")
	m.raftBootstrap = cmdMaster.Flag.Bool("raftBootstrap", false, "是否引导启动 Raft 集群")
	m.telemetryUrl = cmdMaster.Flag.String("telemetry.url", "https://telemetry.seaweedfs.com/api/collect", "用于发送使用统计的遥测服务器 URL")
	m.telemetryEnabled = cmdMaster.Flag.Bool("telemetry", true, "向 telemetry.url 上报匿名集群统计,使用 -telemetry=false 可退出")
	m.debug = cmdMaster.Flag.Bool("debug", false, "通过 pprof 在 -debug.port 指定的端口上提供运行时性能分析数据")
	m.debugPort = cmdMaster.Flag.Int("debug.port", 6060, "用于调试的 http 端口")
}

var cmdMaster = &Command{
	UsageLine: "master -port=9333",
	Short:     "启动 master 服务器",
	Long: `启动一个 master 服务器,提供 volume=>location 映射服务以及文件 ID 的序列号

	配置文件 "security.toml" 会按以下顺序读取:"."、"$HOME/.seaweedfs/"、"/usr/local/etc/seaweedfs/" 或 "/etc/seaweedfs/"。

	示例 security.toml 配置文件可通过 "weed scaffold -config=security" 生成

	对于单 master 部署,使用 -peers=none 可跳过 Raft quorum 等待并实现即时启动。
	这非常适合开发或独立部署场景。

  `,
}

var (
	masterCpuProfile = cmdMaster.Flag.String("cpuprofile", "", "CPU 性能分析输出文件")
	masterMemProfile = cmdMaster.Flag.String("memprofile", "", "内存性能分析输出文件")
)

func runMaster(cmd *Command, args []string) bool {
	if *m.debug {
		grace.StartDebugServer(*m.debugPort)
	}

	util.LoadSecurityConfiguration()
	util.LoadConfiguration("master", false)

	// bind viper configuration to command line flags
	if v := util.GetViper().GetString("master.mdir"); v != "" {
		*m.metaFolder = v
	}

	*m.metaFolder = util.ResolvePath(*m.metaFolder)
	*masterCpuProfile = util.ResolvePath(*masterCpuProfile)
	*masterMemProfile = util.ResolvePath(*masterMemProfile)
	grace.SetupProfiling(*masterCpuProfile, *masterMemProfile)

	parent, _ := util.FullPath(*m.metaFolder).DirAndName()
	if util.FileExists(string(parent)) && !util.FileExists(*m.metaFolder) {
		if err := os.MkdirAll(*m.metaFolder, 0755); err != nil {
			glog.Fatalf("Could not create Meta Folder %s: %v", *m.metaFolder, err)
		}
	}
	if err := util.TestFolderWritable(*m.metaFolder); err != nil {
		glog.Fatalf("Check Meta Folder (-mdir) Writable %s : %s", *m.metaFolder, err)
	}

	masterWhiteList := util.StringSplit(*m.whiteList, ",")
	if *m.volumeSizeLimitMB > util.VolumeSizeLimitGB*1000 {
		glog.Fatalf("volumeSizeLimitMB should be smaller than 30000")
	}

	switch {
	case *m.metricsHttpIp != "":
		// noting to do, use m.metricsHttpIp
	case *m.ipBind != "":
		*m.metricsHttpIp = *m.ipBind
	case *m.ip != "":
		*m.metricsHttpIp = *m.ip
	}
	go stats_collect.StartMetricsServer(*m.metricsHttpIp, *m.metricsHttpPort)
	go stats_collect.LoopPushingMetric("masterServer", util.JoinHostPort(*m.ip, *m.port), *m.metricsAddress, *m.metricsIntervalSec)
	startMaster(m, masterWhiteList)
	return true
}

func startMaster(masterOption MasterOptions, masterWhiteList []string) {

	backend.LoadConfiguration(util.GetViper())

	if *masterOption.portGrpc == 0 {
		*masterOption.portGrpc = 10000 + *masterOption.port
	}
	if *masterOption.ipBind == "" {
		*masterOption.ipBind = *masterOption.ip
	}
	util.SetOutboundLocalIP(*masterOption.ipBind)

	myMasterAddress, peers := checkPeers(*masterOption.ip, *masterOption.port, *masterOption.portGrpc, *masterOption.peers)

	masterPeers := make(map[string]pb.ServerAddress)
	for _, peer := range peers {
		masterPeers[string(peer)] = peer
	}

	r := mux.NewRouter()
	ms := weed_server.NewMasterServer(r, masterOption.toMasterOption(masterWhiteList), masterPeers)
	listeningAddress := util.JoinHostPort(*masterOption.ipBind, *masterOption.port)
	glog.V(0).Infof("Start Seaweed Master %s at %s", version.Version(), listeningAddress)
	masterListener, masterLocalListener, e := util.NewIpAndLocalListeners(*masterOption.ipBind, *masterOption.port, 0)
	if e != nil {
		glog.Fatalf("Master startup error: %v", e)
	}

	// start raftServer
	metaDir := path.Join(*masterOption.metaFolder, fmt.Sprintf("m%d", *masterOption.port))

	isSingleMaster := isSingleMasterMode(*masterOption.peers)

	raftServerOption := &weed_server.RaftServerOption{
		GrpcDialOption:    security.LoadClientTLS(util.GetViper(), "grpc.master"),
		Peers:             masterPeers,
		ServerAddr:        myMasterAddress,
		DataDir:           util.ResolvePath(metaDir),
		Topo:              ms.Topo,
		RaftResumeState:   *masterOption.raftResumeState,
		SingleMaster:      isSingleMaster,
		HeartbeatInterval: *masterOption.heartbeatInterval,
		ElectionTimeout:   *masterOption.electionTimeout,
		RaftBootstrap:     *masterOption.raftBootstrap,
	}
	var raftServer *weed_server.RaftServer
	var err error
	if *masterOption.raftHashicorp {
		if raftServer, err = weed_server.NewHashicorpRaftServer(raftServerOption); err != nil {
			glog.Fatalf("NewHashicorpRaftServer: %s", err)
		}
	} else {
		raftServer, err = weed_server.NewRaftServer(raftServerOption)
		if raftServer == nil {
			glog.Fatalf("please verify %s is writable, see https://github.com/seaweedfs/seaweedfs/issues/717: %s", *masterOption.metaFolder, err)
		}
		// For single-master mode with a fresh log, initialize cluster immediately.
		// When resuming with existing state, the server is already a member and
		// will self-elect via fastResume — sending another JoinCommand would block
		// because goraft's setCommitIndex returns early on JoinCommand entries,
		// preventing the new entry's event from being notified when old uncommitted
		// JoinCommands exist in the log.
		if isSingleMaster && !raftServer.HasExistingState() {
			glog.V(0).Infof("Single-master mode: initializing cluster immediately")
			raftServer.DoJoinCommand()
		}
	}
	ms.SetRaftServer(raftServer)
	r.HandleFunc("/cluster/status", raftServer.StatusHandler).Methods(http.MethodGet, http.MethodHead)
	r.HandleFunc("/cluster/healthz", raftServer.HealthzHandler).Methods(http.MethodGet, http.MethodHead)
	if *masterOption.raftHashicorp {
		r.HandleFunc("/raft/stats", raftServer.StatsRaftHandler).Methods(http.MethodGet)
	}
	// starting grpc server
	grpcPort := *masterOption.portGrpc
	grpcL, grpcLocalL, err := util.NewIpAndLocalListeners(*masterOption.ipBind, grpcPort, 0)
	if err != nil {
		glog.Fatalf("master failed to listen on grpc port %d: %v", grpcPort, err)
	}
	grpcS := pb.NewGrpcServer(security.LoadServerTLS(util.GetViper(), "grpc.master"))
	master_pb.RegisterSeaweedServer(grpcS, ms)
	if *masterOption.raftHashicorp {
		raftServer.TransportManager.Register(grpcS)
	} else {
		protobuf.RegisterRaftServer(grpcS, raftServer)
	}
	reflection.Register(grpcS)
	glog.V(0).Infof("Start Seaweed Master %s grpc server at %s:%d", version.Version(), *masterOption.ipBind, grpcPort)
	if grpcLocalL != nil {
		go grpcS.Serve(grpcLocalL)
	}
	go grpcS.Serve(grpcL)
	pb.ServeGrpcOnLocalSocket(grpcS, grpcPort)

	// For multi-master mode with non-Hashicorp raft, wait and check if we should join
	if !*masterOption.raftHashicorp && !isSingleMaster {
		go func() {
			// Stagger bootstrap by peer index so masters don't all check
			// simultaneously. Peer 0 waits ~1.5s, peer 1 ~3s, etc.
			idx := peerIndex(myMasterAddress, peers)
			delay := time.Duration(float64(raftJoinCheckDelay) * (rand.Float64()*0.25 + 1) * float64(idx+1))
			glog.V(0).Infof("bootstrap check in %v (peer index %d of %d)", delay, idx, len(peers))
			time.Sleep(delay)

			ms.Topo.RaftServerAccessLock.RLock()
			isEmptyMaster := ms.Topo.RaftServer.Leader() == "" && ms.Topo.RaftServer.IsLogEmpty()
			isFirst := idx == 0
			if isEmptyMaster && isFirst {
				existingLeader := ms.MasterClient.FindLeaderFromOtherPeers(myMasterAddress)
				if existingLeader == "" {
					raftServer.DoJoinCommand()
				} else {
					glog.V(0).Infof("skip bootstrap: existing leader %s found from peers", existingLeader)
				}
			} else if !isEmptyMaster {
				glog.V(0).Infof("skip bootstrap: leader=%q logEmpty=%v", ms.Topo.RaftServer.Leader(), ms.Topo.RaftServer.IsLogEmpty())
			} else {
				glog.V(0).Infof("skip bootstrap: %v is not the first master in peers (index %d)", myMasterAddress, idx)
			}
			ms.Topo.RaftServerAccessLock.RUnlock()
		}()
	}

	go ms.MasterClient.KeepConnectedToMaster(context.Background())

	// start http server
	var (
		clientCertFile,
		certFile,
		keyFile string
	)
	useTLS := false
	useMTLS := false

	if viper.GetString("https.master.key") != "" {
		useTLS = true
		certFile = viper.GetString("https.master.cert")
		keyFile = viper.GetString("https.master.key")
	}

	if viper.GetString("https.master.ca") != "" {
		useMTLS = true
		clientCertFile = viper.GetString("https.master.ca")
	}

	if masterLocalListener != nil {
		go newHttpServer(r, nil).Serve(masterLocalListener)
	}

	var tlsConfig *tls.Config
	if useMTLS {
		tlsConfig = security.LoadClientTLSHTTP(clientCertFile)
		if err := security.FixTlsConfig(util.GetViper(), tlsConfig); err != nil {
			glog.Fatalf("failed to fix TLS config: %v", err)
		}
	}

	if useTLS {
		getCert, certProvider, err := security.NewReloadingServerCertificate(certFile, keyFile)
		if err != nil {
			glog.Fatalf("failed to load master HTTPS certificate: %v", err)
		}
		// Master runs ServeTLS in a goroutine and this function then blocks
		// on shutdownCtx / select{}; tie the pem refresh goroutine to the
		// existing interrupt hook instead of a local defer that would fire
		// while the server is still running.
		grace.OnInterrupt(certProvider.Close)
		if tlsConfig == nil {
			tlsConfig = &tls.Config{}
		}
		tlsConfig.GetCertificate = getCert
		go newHttpServer(r, tlsConfig).ServeTLS(masterListener, "", "")
	} else {
		go newHttpServer(r, nil).Serve(masterListener)
	}

	grace.OnInterrupt(ms.Shutdown)
	grace.OnInterrupt(grpcS.Stop)
	grace.OnReload(ms.Reload)
	grace.OnReload(func() {
		if ms.Topo.HashicorpRaft != nil && ms.Topo.HashicorpRaft.State() == hashicorpRaft.Leader {
			ms.Topo.HashicorpRaft.LeadershipTransfer()
		}
	})
	if masterOption.shutdownCtx != nil {
		<-masterOption.shutdownCtx.Done()
		ms.Shutdown()
		grpcS.Stop()
	} else {
		select {}
	}
}

func isSingleMasterMode(peers string) bool {
	p := strings.ToLower(strings.TrimSpace(peers))
	return p == "none"
}

func checkPeers(masterIp string, masterPort int, masterGrpcPort int, peers string) (masterAddress pb.ServerAddress, cleanedPeers []pb.ServerAddress) {
	glog.V(0).Infof("current: %s:%d peers:%s", masterIp, masterPort, peers)
	masterAddress = pb.NewServerAddress(masterIp, masterPort, masterGrpcPort)

	// Handle special case: -peers=none for single-master setup
	if isSingleMasterMode(peers) {
		glog.V(0).Infof("Running in single-master mode (peers=none), no quorum required")
		cleanedPeers = []pb.ServerAddress{masterAddress}
		return
	}

	peers = strings.TrimSpace(peers)
	seenPeers := make(map[string]struct{})
	for _, peer := range pb.ServerAddresses(peers).ToAddresses() {
		normalizedPeer := normalizeMasterPeerAddress(peer, masterAddress)
		key := string(normalizedPeer)
		if _, found := seenPeers[key]; found {
			continue
		}
		seenPeers[key] = struct{}{}
		cleanedPeers = append(cleanedPeers, normalizedPeer)
	}

	hasSelf := false
	for _, peer := range cleanedPeers {
		if peer.ToHttpAddress() == masterAddress.ToHttpAddress() {
			hasSelf = true
			break
		}
	}

	if !hasSelf {
		cleanedPeers = append(cleanedPeers, masterAddress)
	}
	if len(cleanedPeers)%2 == 0 {
		glog.Fatalf("Only odd number of masters are supported: %+v", cleanedPeers)
	}
	return
}

func normalizeMasterPeerAddress(peer pb.ServerAddress, self pb.ServerAddress) pb.ServerAddress {
	if peer.ToHttpAddress() == self.ToHttpAddress() {
		return self
	}

	_, grpcPort, err := net.SplitHostPort(peer.ToGrpcAddress())
	if err != nil {
		return peer
	}
	grpcPortValue, err := strconv.Atoi(grpcPort)
	if err != nil {
		return peer
	}

	return pb.NewServerAddressWithGrpcPort(peer.ToHttpAddress(), grpcPortValue)
}

// peerIndex returns the 0-based position of self in the sorted peer list.
// Peer 0 is the designated bootstrap node. Returns -1 if self is not found.
func peerIndex(self pb.ServerAddress, peers []pb.ServerAddress) int {
	slices.SortFunc(peers, func(a, b pb.ServerAddress) int {
		return strings.Compare(a.ToHttpAddress(), b.ToHttpAddress())
	})
	for i, peer := range peers {
		if peer.ToHttpAddress() == self.ToHttpAddress() {
			return i
		}
	}
	glog.Warningf("peerIndex: self %s not found in peers %v", self, peers)
	return -1
}

func (m *MasterOptions) toMasterOption(whiteList []string) *weed_server.MasterOption {
	masterAddress := pb.NewServerAddress(*m.ip, *m.port, *m.portGrpc)
	return &weed_server.MasterOption{
		Master:                     masterAddress,
		MetaFolder:                 *m.metaFolder,
		VolumeSizeLimitMB:          uint32(*m.volumeSizeLimitMB),
		VolumePreallocate:          *m.volumePreallocate,
		MaxParallelVacuumPerServer: *m.maxParallelVacuumPerServer,
		// PulseSeconds:            *m.pulseSeconds,
		DefaultReplicaPlacement: *m.defaultReplication,
		GarbageThreshold:        *m.garbageThreshold,
		WhiteList:               whiteList,
		DisableHttp:             *m.disableHttp,
		MetricsAddress:          *m.metricsAddress,
		MetricsIntervalSec:      *m.metricsIntervalSec,
		TelemetryUrl:            *m.telemetryUrl,
		TelemetryEnabled:        *m.telemetryEnabled,
	}
}
