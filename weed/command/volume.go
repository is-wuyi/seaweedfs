package command

import (
	"context"
	"crypto/tls"
	"net/http"
	httppprof "net/http/pprof"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	weed_server "github.com/seaweedfs/seaweedfs/weed/server"
	"github.com/seaweedfs/seaweedfs/weed/server/constants"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"
	"github.com/seaweedfs/seaweedfs/weed/util/httpdown"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
)

var (
	v VolumeServerOptions
)

type VolumeServerOptions struct {
	port                      *int
	portGrpc                  *int
	publicPort                *int
	folders                   []string
	folderMaxLimits           []int32
	idxFolder                 *string
	ip                        *string
	id                        *string
	publicUrl                 *string
	bindIp                    *string
	mastersString             *string
	mserverString             *string // deprecated, for backward compatibility
	masters                   []pb.ServerAddress
	idleConnectionTimeout     *int
	dataCenter                *string
	rack                      *string
	whiteList                 []string
	indexType                 *string
	diskType                  *string
	tags                      *string
	fixJpgOrientation         *bool
	readMode                  *string
	cpuProfile                *string
	memProfile                *string
	compactionMBPerSecond     *int
	maintenanceMBPerSecond    *int
	fileSizeLimitMB           *int
	concurrentUploadLimitMB   *int
	concurrentDownloadLimitMB *int
	pprof                     *bool
	preStopSeconds            *int
	metricsHttpPort           *int
	metricsHttpIp             *string
	// pulseSeconds          *int
	inflightUploadDataTimeout     *time.Duration
	inflightDownloadDataTimeout   *time.Duration
	hasSlowRead                   *bool
	readBufferSizeMB              *int
	ldbTimeout                    *int64
	allowUntrustedRemoteEndpoints *bool
	debug                         *bool
	debugPort                     *int
	diskIOProbe                   *bool
	diskIOTimeout                 *time.Duration
	diskIOInterval                *time.Duration
	diskHDDIOSlowLatency          *time.Duration
	diskSSDIOSlowLatency          *time.Duration
	diskNVMEIOSlowLatency         *time.Duration
	diskIOWindow                  *time.Duration
	diskIOMinSamples              *int
	diskIOSlowPercent             *float64
	diskIOErrorPercent            *float64
	diskIOMaxStatFailures         *int
	diskRecoveryCoef              *float64
	// shutdownCtx, when non-nil, tells startVolumeServer to shut down once the
	// ctx is cancelled. Used by integration tests and by weed mini; nil for
	// standalone weed volume.
	shutdownCtx context.Context
}

func (v *VolumeServerOptions) setDiskIOProbeDefaults() {
	if v.diskIOProbe == nil {
		defaultValue := false
		v.diskIOProbe = &defaultValue
	}
	if v.diskIOTimeout == nil {
		defaultValue := 2 * time.Second
		v.diskIOTimeout = &defaultValue
	}
	if v.diskIOInterval == nil {
		defaultValue := 60 * time.Second
		v.diskIOInterval = &defaultValue
	}
	if v.diskHDDIOSlowLatency == nil {
		defaultValue := 500 * time.Millisecond
		v.diskHDDIOSlowLatency = &defaultValue
	}
	if v.diskSSDIOSlowLatency == nil {
		defaultValue := 100 * time.Millisecond
		v.diskSSDIOSlowLatency = &defaultValue
	}
	if v.diskNVMEIOSlowLatency == nil {
		defaultValue := 50 * time.Millisecond
		v.diskNVMEIOSlowLatency = &defaultValue
	}
	if v.diskIOWindow == nil {
		defaultValue := time.Minute
		v.diskIOWindow = &defaultValue
	}
	if v.diskIOMinSamples == nil {
		defaultValue := 10
		v.diskIOMinSamples = &defaultValue
	}
	if v.diskIOSlowPercent == nil {
		defaultValue := 20.0
		v.diskIOSlowPercent = &defaultValue
	}
	if v.diskIOErrorPercent == nil {
		defaultValue := 10.0
		v.diskIOErrorPercent = &defaultValue
	}
	if v.diskIOMaxStatFailures == nil {
		defaultValue := 5
		v.diskIOMaxStatFailures = &defaultValue
	}
	if v.diskRecoveryCoef == nil {
		defaultValue := 0.5
		v.diskRecoveryCoef = &defaultValue
	}
}

func (v *VolumeServerOptions) applyDiskIOProbeConfig() {
	v.setDiskIOProbeDefaults()

	config := util.GetViper()

	if config.IsSet("volume.disk.io.probe") {
		*v.diskIOProbe = config.GetBool("volume.disk.io.probe")
	}
	if config.IsSet("volume.disk.io.timeout") {
		*v.diskIOTimeout = config.GetDuration("volume.disk.io.timeout")
	}
	if config.IsSet("volume.disk.io.slow.latency.hdd") {
		*v.diskHDDIOSlowLatency = config.GetDuration("volume.disk.io.slow.latency.hdd")
	}
	if config.IsSet("volume.disk.io.slow.latency.ssd") {
		*v.diskSSDIOSlowLatency = config.GetDuration("volume.disk.io.slow.latency.ssd")
	}
	if config.IsSet("volume.disk.io.slow.latency.nvme") {
		*v.diskNVMEIOSlowLatency = config.GetDuration("volume.disk.io.slow.latency.nvme")
	}

	if config.IsSet("volume.disk.io.interval") {
		*v.diskIOInterval = config.GetDuration("volume.disk.io.interval")
	}
	if config.IsSet("volume.disk.io.window") {
		*v.diskIOWindow = config.GetDuration("volume.disk.io.window")
	}
	if config.IsSet("volume.disk.io.min.samples") {
		*v.diskIOMinSamples = config.GetInt("volume.disk.io.min.samples")
	}
	if config.IsSet("volume.disk.io.slow.percent") {
		*v.diskIOSlowPercent = config.GetFloat64("volume.disk.io.slow.percent")
	}
	if config.IsSet("volume.disk.io.error.percent") {
		*v.diskIOErrorPercent = config.GetFloat64("volume.disk.io.error.percent")
	}
	if config.IsSet("volume.disk.io.max.stat.failures") {
		*v.diskIOMaxStatFailures = config.GetInt("volume.disk.io.max.stat.failures")
	}
	if config.IsSet("volume.disk.io.recovery.coef") {
		*v.diskRecoveryCoef = config.GetFloat64("volume.disk.io.recovery.coef")
	}
}

func init() {
	cmdVolume.Run = runVolume // break init cycle
	v.port = cmdVolume.Flag.Int("port", 8080, "http 监听端口")
	v.portGrpc = cmdVolume.Flag.Int("port.grpc", 0, "grpc 监听端口")
	v.publicPort = cmdVolume.Flag.Int("port.public", 0, "对公众开放的端口")
	v.ip = cmdVolume.Flag.String("ip", util.DetectedHostAddress(), "IP 或服务器名,同时用作标识符")
	v.id = cmdVolume.Flag.String("id", "", "volume 服务器 ID。为空则默认为 ip:port")
	v.publicUrl = cmdVolume.Flag.String("publicUrl", "", "可公开访问的地址")
	v.bindIp = cmdVolume.Flag.String("ip.bind", "", "绑定的 IP 地址。为空则默认与 -ip 选项相同。")
	v.mastersString = cmdVolume.Flag.String("master", "localhost:9333", "逗号分隔的 master 服务器")
	v.mserverString = cmdVolume.Flag.String("mserver", "", "逗号分隔的 master 服务器(已废弃,请改用 -master)")
	v.preStopSeconds = cmdVolume.Flag.Int("preStopSeconds", 10, "停止发送心跳与停止 volume 服务器之间的秒数")
	// v.pulseSeconds = cmdVolume.Flag.Int("pulseSeconds", 5, "心跳之间的秒数,必须小于或等于 master 的设置")
	v.idleConnectionTimeout = cmdVolume.Flag.Int("idleTimeout", 30, "连接空闲秒数")
	v.dataCenter = cmdVolume.Flag.String("dataCenter", "", "当前 volume 服务器的数据中心名称")
	v.rack = cmdVolume.Flag.String("rack", "", "当前 volume 服务器的机架名称")
	v.indexType = cmdVolume.Flag.String("index", "memory", "选择 [memory|leveldb|leveldbMedium|leveldbLarge] 模式以平衡内存与性能。")
	v.diskType = cmdVolume.Flag.String("disk", "", "[hdd|ssd|<tag>] 机械硬盘、固态硬盘或任意标签")
	v.tags = cmdVolume.Flag.String("tags", "", "每个数据目录的逗号分隔标签组;每组使用 ':'(例如 fast:ssd,archive)")
	v.fixJpgOrientation = cmdVolume.Flag.Bool("images.fix.orientation", false, "上传时调整 jpg 方向。")
	v.readMode = cmdVolume.Flag.String("readMode", "proxy", "[local|proxy|redirect] 如何处理非本地 volume:'未找到|代理到远程节点|重定向 volume 位置'。")
	v.cpuProfile = cmdVolume.Flag.String("cpuprofile", "", "CPU 性能分析输出文件")
	v.memProfile = cmdVolume.Flag.String("memprofile", "", "内存性能分析输出文件")
	v.compactionMBPerSecond = cmdVolume.Flag.Int("compactionMBps", 0, "限制后台 compact 或复制速度,单位为 MB 每秒")
	v.maintenanceMBPerSecond = cmdVolume.Flag.Int("maintenanceMBps", 0, "限制维护(副本/均衡)IO 速率,单位为 MB/s。未设置为 0,即不限制。")
	v.fileSizeLimitMB = cmdVolume.Flag.Int("fileSizeLimitMB", 256, "限制文件大小以避免内存不足")
	v.ldbTimeout = cmdVolume.Flag.Int64("index.leveldbTimeout", 0, "alive time for leveldb (default to 0). If leveldb of volume is not accessed in ldbTimeout hours, it will be off loaded to reduce opened files and memory consumption.")
	v.concurrentUploadLimitMB = cmdVolume.Flag.Int("concurrentUploadLimitMB", 0, "限制总并发上传大小,0 表示不限制")
	v.concurrentDownloadLimitMB = cmdVolume.Flag.Int("concurrentDownloadLimitMB", 0, "限制总并发下载大小,0 表示不限制")
	v.pprof = cmdVolume.Flag.Bool("pprof", false, "启用 pprof http 处理器。与 -memprofile 和 -cpuprofile 互斥")
	v.metricsHttpPort = cmdVolume.Flag.Int("metricsPort", 0, "Prometheus 指标监听端口")
	v.metricsHttpIp = cmdVolume.Flag.String("metricsIp", "", "指标监听 IP。为空则默认与 -ip.bind 选项相同。")
	v.idxFolder = cmdVolume.Flag.String("dir.idx", "", "存储 .idx 文件的目录")
	v.inflightUploadDataTimeout = cmdVolume.Flag.Duration("inflightUploadDataTimeout", 60*time.Second, "volume 服务器的在途上传数据等待超时")
	v.inflightDownloadDataTimeout = cmdVolume.Flag.Duration("inflightDownloadDataTimeout", 60*time.Second, "volume 服务器的在途下载数据等待超时")
	v.hasSlowRead = cmdVolume.Flag.Bool("hasSlowRead", true, "<实验性> 若为 true,可防止慢读阻塞其他请求,但大文件读取 P99 延迟将增加。")
	v.readBufferSizeMB = cmdVolume.Flag.Int("readBufferSizeMB", 4, "<实验性> 较大的值可以优化查询性能,但会增加一些内存使用,通常与 hasSlowRead 配合使用。")
	v.allowUntrustedRemoteEndpoints = cmdVolume.Flag.Bool("volume.allowUntrustedRemoteEndpoints", false, "若为 true,FetchAndWriteNeedle 将接受任意远程 S3 端点,包括回环/链路本地主机。默认拒绝内部/元数据端点。")
	v.debug = cmdVolume.Flag.Bool("debug", false, "通过 pprof 在 -debug.port 指定的端口上提供运行时性能分析数据")
	v.debugPort = cmdVolume.Flag.Int("debug.port", 6060, "用于调试的 http 端口")
	v.setDiskIOProbeDefaults()
}

var cmdVolume = &Command{
	UsageLine: "volume -port=8080 -dir=/tmp -max=5 -ip=server_name -master=localhost:9333",
	Short:     "启动 volume 服务器",
	Long: `启动一个 volume 服务器以提供存储空间

  `,
}

var (
	volumeFolders         = cmdVolume.Flag.String("dir", os.TempDir(), "存储数据文件的目录。dir[,dir]...")
	maxVolumeCounts       = cmdVolume.Flag.String("max", "8", "最大 volume 数量,count[,count]... 设为 0 时,将按可用磁盘空间除以 volume 大小自动配置限制。")
	volumeWhiteListOption = cmdVolume.Flag.String("whiteList", "", "逗号分隔的具有写权限的 IP 地址列表,为空则不限制")
	minFreeSpacePercent   = cmdVolume.Flag.String("minFreeSpacePercent", "1", "最小可用磁盘空间(默认为 1%)。磁盘空间不足会将所有 volume 标记为只读(已废弃,请改用 minFreeSpace)。")
	minFreeSpace          = cmdVolume.Flag.String("minFreeSpace", "", "最小可用磁盘空间(value<=100 视为百分比如 1,其他视为人类可读字节数如 10GiB)。磁盘空间不足会将所有 volume 标记为只读。")
)

func runVolume(cmd *Command, args []string) bool {
	if *v.debug {
		grace.StartDebugServer(*v.debugPort)
	}

	util.LoadSecurityConfiguration()
	util.LoadConfiguration("volume", false)
	v.applyDiskIOProbeConfig()

	// If --pprof is set we assume the caller wants to be able to collect
	// cpu and memory profiles via go tool pprof
	if !*v.pprof {
		*v.cpuProfile = util.ResolvePath(*v.cpuProfile)
		*v.memProfile = util.ResolvePath(*v.memProfile)
		grace.SetupProfiling(*v.cpuProfile, *v.memProfile)
	}

	switch {
	case *v.metricsHttpIp != "":
		// noting to do, use v.metricsHttpIp
	case *v.bindIp != "":
		*v.metricsHttpIp = *v.bindIp
	case *v.ip != "":
		*v.metricsHttpIp = *v.ip
	}
	go stats_collect.StartMetricsServer(*v.metricsHttpIp, *v.metricsHttpPort)

	// Backward compatibility: if -mserver is provided, use it
	if *v.mserverString != "" {
		*v.mastersString = *v.mserverString
	}

	minFreeSpaces := util.MustParseMinFreeSpace(*minFreeSpace, *minFreeSpacePercent)
	v.masters = pb.ServerAddresses(*v.mastersString).ToAddresses()
	v.startVolumeServer(*volumeFolders, *maxVolumeCounts, *volumeWhiteListOption, minFreeSpaces)

	return true
}

func (v VolumeServerOptions) startVolumeServer(volumeFolders, maxVolumeCounts, volumeWhiteListOption string, minFreeSpaces []util.MinFreeSpace) {
	v.setDiskIOProbeDefaults()

	// Set multiple folders and each folder's max volume count limit'
	v.folders = strings.Split(volumeFolders, ",")
	for i, folder := range v.folders {
		v.folders[i] = util.ResolvePath(folder)
		if err := util.TestFolderWritable(v.folders[i]); err != nil {
			glog.Fatalf("Check Data Folder(-dir) Writable %s : %s", v.folders[i], err)
		}
	}

	// set max
	maxCountStrings := strings.Split(maxVolumeCounts, ",")
	for _, maxString := range maxCountStrings {
		if max, e := strconv.ParseInt(maxString, 10, 64); e == nil {
			v.folderMaxLimits = append(v.folderMaxLimits, int32(max))
		} else {
			glog.Fatalf("The max specified in -max not a valid number %s", maxString)
		}
	}
	if len(v.folderMaxLimits) == 1 && len(v.folders) > 1 {
		for i := 0; i < len(v.folders)-1; i++ {
			v.folderMaxLimits = append(v.folderMaxLimits, v.folderMaxLimits[0])
		}
	}
	if len(v.folders) != len(v.folderMaxLimits) {
		glog.Fatalf("%d directories by -dir, but only %d max is set by -max", len(v.folders), len(v.folderMaxLimits))
	}

	if len(minFreeSpaces) == 1 && len(v.folders) > 1 {
		for i := 0; i < len(v.folders)-1; i++ {
			minFreeSpaces = append(minFreeSpaces, minFreeSpaces[0])
		}
	}
	if len(v.folders) != len(minFreeSpaces) {
		glog.Fatalf("%d directories by -dir, but only %d minFreeSpacePercent is set by -minFreeSpacePercent", len(v.folders), len(minFreeSpaces))
	}

	// set disk types
	var diskTypes []types.DiskType
	diskTypeStrings := strings.Split(*v.diskType, ",")
	for _, diskTypeString := range diskTypeStrings {
		diskTypes = append(diskTypes, types.ToDiskType(diskTypeString))
	}
	if len(diskTypes) == 1 && len(v.folders) > 1 {
		for i := 0; i < len(v.folders)-1; i++ {
			diskTypes = append(diskTypes, diskTypes[0])
		}
	}
	if len(v.folders) != len(diskTypes) {
		glog.Fatalf("%d directories by -dir, but only %d disk types is set by -disk", len(v.folders), len(diskTypes))
	}

	var tagsArg string
	if v.tags != nil {
		tagsArg = *v.tags
	}
	folderTags := parseVolumeTags(tagsArg, len(v.folders))

	// security related white list configuration
	v.whiteList = util.StringSplit(volumeWhiteListOption, ",")

	if *v.ip == "" {
		*v.ip = util.DetectedHostAddress()
		glog.V(0).Infof("detected volume server ip address: %v", *v.ip)
	}
	if *v.bindIp == "" {
		*v.bindIp = *v.ip
	}
	util.SetOutboundLocalIP(*v.bindIp)

	if *v.publicPort == 0 {
		*v.publicPort = *v.port
	}
	if *v.portGrpc == 0 {
		*v.portGrpc = 10000 + *v.port
	}
	if *v.publicUrl == "" {
		*v.publicUrl = util.JoinHostPort(*v.ip, *v.publicPort)
	}

	volumeMux := http.NewServeMux()
	publicVolumeMux := volumeMux
	if v.isSeparatedPublicPort() {
		publicVolumeMux = http.NewServeMux()
	}

	if *v.pprof {
		volumeMux.HandleFunc("/debug/pprof/", httppprof.Index)
		volumeMux.HandleFunc("/debug/pprof/cmdline", httppprof.Cmdline)
		volumeMux.HandleFunc("/debug/pprof/profile", httppprof.Profile)
		volumeMux.HandleFunc("/debug/pprof/symbol", httppprof.Symbol)
		volumeMux.HandleFunc("/debug/pprof/trace", httppprof.Trace)
	}

	volumeNeedleMapKind := storage.NeedleMapInMemory
	switch *v.indexType {
	case "leveldb":
		volumeNeedleMapKind = storage.NeedleMapLevelDb
	case "leveldbMedium":
		volumeNeedleMapKind = storage.NeedleMapLevelDbMedium
	case "leveldbLarge":
		volumeNeedleMapKind = storage.NeedleMapLevelDbLarge
	}

	// Determine volume server ID: if not specified, use ip:port
	volumeServerId := util.GetVolumeServerId(*v.id, *v.ip, *v.port)
	var slowLatency time.Duration

	switch *v.diskType {
	case "hdd":
		slowLatency = *v.diskHDDIOSlowLatency
	case "ssd":
		slowLatency = *v.diskSSDIOSlowLatency
	case "nvme":
		slowLatency = *v.diskNVMEIOSlowLatency
	default:
		slowLatency = *v.diskHDDIOSlowLatency
	}
	diskProbeConfig := stats_collect.DiskIOProbeConfig{
		Enabled:  *v.diskIOProbe,
		Timeout:  *v.diskIOTimeout,
		Interval: *v.diskIOInterval,

		SlowLatency: slowLatency,

		Window:     *v.diskIOWindow,
		MinSamples: *v.diskIOMinSamples,

		SlowPercent:  *v.diskIOSlowPercent,
		ErrorPercent: *v.diskIOErrorPercent,

		MaxStatFailures: *v.diskIOMaxStatFailures,

		RecoveryCoef: *v.diskRecoveryCoef,
	}
	if diskProbeConfig.Enabled && len(v.folders) > 1 {
		glog.Warningf("disk IO probe is disabled for multiple volume directories: %v", v.folders)
		diskProbeConfig.Enabled = false
	}
	volumeServer := weed_server.NewVolumeServer(volumeMux, publicVolumeMux,
		*v.ip, *v.port, *v.portGrpc, *v.publicUrl, volumeServerId,
		v.folders, v.folderMaxLimits, minFreeSpaces, diskTypes, folderTags,
		util.ResolvePath(*v.idxFolder),
		volumeNeedleMapKind,
		v.masters, constants.VolumePulsePeriod, *v.dataCenter, *v.rack,
		v.whiteList,
		*v.fixJpgOrientation, *v.readMode,
		*v.compactionMBPerSecond,
		*v.maintenanceMBPerSecond,
		*v.fileSizeLimitMB,
		int64(*v.concurrentUploadLimitMB)*1024*1024,
		int64(*v.concurrentDownloadLimitMB)*1024*1024,
		*v.inflightUploadDataTimeout,
		*v.inflightDownloadDataTimeout,
		*v.hasSlowRead,
		*v.readBufferSizeMB,
		*v.ldbTimeout,
		*v.allowUntrustedRemoteEndpoints,
		diskProbeConfig,
	)
	// starting grpc server
	grpcS := v.startGrpcService(volumeServer)

	// starting public http server
	var publicHttpDown httpdown.Server
	if v.isSeparatedPublicPort() {
		publicHttpDown = v.startPublicHttpService(publicVolumeMux)
		if nil == publicHttpDown {
			glog.Fatalf("start public http service failed")
		}
	}

	// starting the cluster http server
	clusterHttpServer, closeCert := v.startClusterHttpService(volumeMux)

	grace.OnReload(volumeServer.LoadNewVolumes)
	grace.OnReload(volumeServer.Reload)

	stopChan := make(chan bool)
	grace.OnInterrupt(func() {
		glog.Infof("volume server has been killed")

		// Stop heartbeats
		if !volumeServer.StopHeartbeat() {
			volumeServer.SetStopping()
			glog.V(0).Infof("stop send heartbeat and wait %d seconds until shutdown ...", *v.preStopSeconds)
			time.Sleep(time.Duration(*v.preStopSeconds) * time.Second)
		}

		shutdown(publicHttpDown, clusterHttpServer, grpcS, volumeServer)
		if closeCert != nil {
			closeCert()
		}
		stopChan <- true
	})

	if v.shutdownCtx != nil {
		select {
		case <-stopChan:
		case <-v.shutdownCtx.Done():
			shutdown(publicHttpDown, clusterHttpServer, grpcS, volumeServer)
			if closeCert != nil {
				closeCert()
			}
		}
	} else {
		select {
		case <-stopChan:
		}
	}

}

func parseVolumeTags(tagsArg string, folderCount int) [][]string {
	if folderCount <= 0 {
		return nil
	}
	tagEntries := []string{}
	if strings.TrimSpace(tagsArg) != "" {
		tagEntries = strings.Split(tagsArg, ",")
	}
	folderTags := make([][]string, folderCount)

	// If exactly one tag entry provided, replicate it to all folders
	if len(tagEntries) == 1 {
		normalized := util.NormalizeTagList(strings.Split(tagEntries[0], ":"))
		for i := 0; i < folderCount; i++ {
			folderTags[i] = append([]string(nil), normalized...)
		}
	} else {
		// Otherwise, assign tags to folders that have explicit entries
		for i := 0; i < folderCount; i++ {
			if i < len(tagEntries) {
				folderTags[i] = util.NormalizeTagList(strings.Split(tagEntries[i], ":"))
			} else {
				// Initialize remaining folders with empty tag slice
				folderTags[i] = []string{}
			}
		}
	}
	return folderTags
}

func shutdown(publicHttpDown httpdown.Server, clusterHttpServer httpdown.Server, grpcS *grpc.Server, volumeServer *weed_server.VolumeServer) {

	// firstly, stop the public http service to prevent from receiving new user request
	if nil != publicHttpDown {
		glog.V(0).Infof("stop public http server ... ")
		if err := publicHttpDown.Stop(); err != nil {
			glog.Warningf("stop the public http server failed, %v", err)
		}
	}

	glog.V(0).Infof("graceful stop cluster http server ... ")
	if err := clusterHttpServer.Stop(); err != nil {
		glog.Warningf("stop the cluster http server failed, %v", err)
	}

	glog.V(0).Infof("graceful stop gRPC ...")
	grpcS.GracefulStop()

	volumeServer.Shutdown()

	pprof.StopCPUProfile()

}

// check whether configure the public port
func (v VolumeServerOptions) isSeparatedPublicPort() bool {
	return *v.publicPort != *v.port
}

func (v VolumeServerOptions) startGrpcService(vs volume_server_pb.VolumeServerServer) *grpc.Server {
	grpcPort := *v.portGrpc
	grpcL, err := util.NewListener(util.JoinHostPort(*v.bindIp, grpcPort), 0)
	if err != nil {
		glog.Fatalf("failed to listen on grpc port %d: %v", grpcPort, err)
	}
	grpcS := pb.NewGrpcServer(security.LoadServerTLS(util.GetViper(), "grpc.volume"))
	volume_server_pb.RegisterVolumeServerServer(grpcS, vs)
	reflection.Register(grpcS)
	go func() {
		if err := grpcS.Serve(grpcL); err != nil {
			glog.Fatalf("start gRPC service failed, %s", err)
		}
	}()
	pb.ServeGrpcOnLocalSocket(grpcS, grpcPort)
	return grpcS
}

func (v VolumeServerOptions) startPublicHttpService(handler http.Handler) httpdown.Server {
	publicListeningAddress := util.JoinHostPort(*v.bindIp, *v.publicPort)
	glog.V(0).Infoln("Start Seaweed volume server", version.Version(), "public at", publicListeningAddress)
	publicListener, e := util.NewListener(publicListeningAddress, time.Duration(*v.idleConnectionTimeout)*time.Second)
	if e != nil {
		glog.Fatalf("Volume server listener error:%v", e)
	}

	pubHttp := httpdown.HTTP{StopTimeout: 5 * time.Minute, KillTimeout: 5 * time.Minute}
	publicHttpDown := pubHttp.Serve(&http.Server{Handler: handler}, publicListener)
	go func() {
		if err := publicHttpDown.Wait(); err != nil {
			glog.Errorf("public http down wait failed, %v", err)
		}
	}()

	return publicHttpDown
}

// startClusterHttpService starts the volume cluster HTTP server and
// returns it along with a close func for the cert reloader's refresh
// goroutine (nil when HTTPS is disabled). The caller is responsible
// for invoking the close func on every shutdown path — both the
// SIGTERM/grace.OnInterrupt path and the shutdownCtx path used by
// mini/integration tests.
func (v VolumeServerOptions) startClusterHttpService(handler http.Handler) (httpdown.Server, func()) {
	var (
		certFile, keyFile string
	)
	if viper.GetString("https.volume.key") != "" {
		certFile = viper.GetString("https.volume.cert")
		keyFile = viper.GetString("https.volume.key")
	}

	listeningAddress := util.JoinHostPort(*v.bindIp, *v.port)
	glog.V(0).Infof("Start Seaweed volume server %s at %s", version.Version(), listeningAddress)
	listener, e := util.NewListener(listeningAddress, time.Duration(*v.idleConnectionTimeout)*time.Second)
	if e != nil {
		glog.Fatalf("Volume server listener error:%v", e)
	}

	httpDown := httpdown.HTTP{
		KillTimeout: time.Minute,
		StopTimeout: 30 * time.Second,
	}
	httpS := &http.Server{Handler: handler}

	if viper.GetString("https.volume.ca") != "" {
		clientCertFile := viper.GetString("https.volume.ca")
		httpS.TLSConfig = security.LoadClientTLSHTTP(clientCertFile)
		if err := security.FixTlsConfig(util.GetViper(), httpS.TLSConfig); err != nil {
			glog.Fatalf("Could not fix TLS config: %v", err)
		}
	}

	var closeCert func()
	if certFile != "" && keyFile != "" {
		getCert, certProvider, err := security.NewReloadingServerCertificate(certFile, keyFile)
		if err != nil {
			glog.Fatalf("Volume server failed to load TLS certificate: %v", err)
		}
		closeCert = certProvider.Close
		if httpS.TLSConfig == nil {
			httpS.TLSConfig = &tls.Config{}
		}
		httpS.TLSConfig.GetCertificate = getCert
	}

	clusterHttpServer := httpDown.Serve(httpS, listener)
	go func() {
		if e := clusterHttpServer.Wait(); e != nil {
			glog.Fatalf("Volume server fail to serve: %v", e)
		}
	}()
	return clusterHttpServer, closeCert
}
