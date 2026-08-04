package command

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/spf13/viper"
	"google.golang.org/grpc/reflection"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	_ "github.com/seaweedfs/seaweedfs/weed/credential/filer_etc"
	_ "github.com/seaweedfs/seaweedfs/weed/credential/memory"
	_ "github.com/seaweedfs/seaweedfs/weed/credential/postgres"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	weed_server "github.com/seaweedfs/seaweedfs/weed/server"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
)

var (
	f                  FilerOptions
	filerStartS3       *bool
	filerS3Options     S3Options
	filerStartWebDav   *bool
	filerWebDavOptions WebDavOption
	filerStartIam      *bool
	filerIamOptions    IamOptions
	filerStartSftp     *bool
	filerSftpOptions   SftpOptions
)

type FilerOptions struct {
	masters                   *pb.ServerDiscovery
	mastersString             *string
	ip                        *string
	bindIp                    *string
	port                      *int
	portGrpc                  *int
	publicPort                *int
	filerGroup                *string
	collection                *string
	defaultReplicaPlacement   *string
	disableDirListing         *bool
	maxMB                     *int
	dirListingLimit           *int
	dataCenter                *string
	rack                      *string
	enableNotification        *bool
	disableHttp               *bool
	cipher                    *bool
	metricsHttpPort           *int
	metricsHttpIp             *string
	saveToFilerLimit          *int
	defaultLevelDbDirectory   *string
	concurrentUploadLimitMB   *int
	concurrentFileUploadLimit *int
	debug                     *bool
	debugPort                 *int
	localSocket               *string
	showUIDirectoryDelete     *bool
	downloadMaxMBps           *int
	diskType                  *string
	allowedOrigins            *string
	exposeDirectoryData       *bool
	tusBasePath               *string
	s3ConfigFile              *string // optional path to static S3 identity config
	// shutdownCtx, when non-nil, tells startFiler to gracefully shut down its
	// HTTP/gRPC servers once the ctx is cancelled. Used by integration tests
	// and by weed mini; nil for standalone weed filer.
	shutdownCtx context.Context
	// gracefulStopTimeout caps how long startFiler waits for gRPC graceful
	// stop before forcing the server to stop. Zero means the default of 10s.
	gracefulStopTimeout time.Duration
}

func init() {
	cmdFiler.Run = runFiler // break init cycle
	f.mastersString = cmdFiler.Flag.String("master", "localhost:9333", "逗号分隔的 master 服务器,或至少 1 个 master 服务器的单条 DNS SRV 记录,需以 dnssrv+ 为前缀")
	f.filerGroup = cmdFiler.Flag.String("filerGroup", "", "与同一 filerGroup 中的其他 filer 共享元数据")
	f.collection = cmdFiler.Flag.String("collection", "", "所有数据将存储在此默认集合中")
	f.ip = cmdFiler.Flag.String("ip", util.DetectedHostAddress(), "filer 服务器 http 监听 IP 地址")
	f.bindIp = cmdFiler.Flag.String("ip.bind", "", "绑定的 IP 地址。为空则默认与 -ip 选项相同。")
	f.port = cmdFiler.Flag.Int("port", 8888, "filer 服务器 http 监听端口")
	f.portGrpc = cmdFiler.Flag.Int("port.grpc", 0, "filer 服务器 grpc 监听端口")
	f.publicPort = cmdFiler.Flag.Int("port.readonly", 0, "对公众开放的只读端口")
	f.defaultReplicaPlacement = cmdFiler.Flag.String("defaultReplicaPlacement", "", "默认副本策略。未指定时使用 master 设置。")
	f.disableDirListing = cmdFiler.Flag.Bool("disableDirListing", false, "关闭目录列表")
	f.maxMB = cmdFiler.Flag.Int("maxMB", 4, "拆分超过该限制的大文件")
	f.dirListingLimit = cmdFiler.Flag.Int("dirListLimit", 100000, "限制子目录列表大小")
	f.dataCenter = cmdFiler.Flag.String("dataCenter", "", "优先读写此数据中心中的 volume")
	f.rack = cmdFiler.Flag.String("rack", "", "优先写入此机架中的 volume")
	f.disableHttp = cmdFiler.Flag.Bool("disableHttp", false, "禁用 http 请求,仅允许 gRPC 操作")
	f.cipher = cmdFiler.Flag.Bool("encryptVolumeData", false, "加密 volume 服务器上的数据")
	f.metricsHttpPort = cmdFiler.Flag.Int("metricsPort", 0, "Prometheus 指标监听端口")
	f.metricsHttpIp = cmdFiler.Flag.String("metricsIp", "", "指标监听 IP。为空则默认与 -ip.bind 选项相同。")
	f.saveToFilerLimit = cmdFiler.Flag.Int("saveToFilerLimit", 0, "小于此限制的文件将保存到 filer 存储中")
	f.defaultLevelDbDirectory = cmdFiler.Flag.String("defaultStoreDir", ".", "如果 filer.toml 为空,则使用该目录下的内嵌 filer 存储")
	f.concurrentUploadLimitMB = cmdFiler.Flag.Int("concurrentUploadLimitMB", 0, "限制总并发上传大小,0 表示不限制")
	f.concurrentFileUploadLimit = cmdFiler.Flag.Int("concurrentFileUploadLimit", 0, "限制并发文件上传数量,0 表示不限制")
	f.debug = cmdFiler.Flag.Bool("debug", false, "提供运行时性能分析数据,例如 http://localhost:<debug.port>/debug/pprof/goroutine?debug=2")
	f.debugPort = cmdFiler.Flag.Int("debug.port", 6060, "用于调试的 http 端口")
	f.localSocket = cmdFiler.Flag.String("localSocket", "", "默认为 /tmp/seaweedfs-filer-<port>.sock")
	f.showUIDirectoryDelete = cmdFiler.Flag.Bool("ui.deleteDir", true, "启用 filer UI 显示删除目录按钮")
	f.downloadMaxMBps = cmdFiler.Flag.Int("downloadMaxMBps", 0, "每个下载请求的最大下载速度,单位为 MB 每秒")
	f.diskType = cmdFiler.Flag.String("disk", "", "[hdd|ssd|<tag>] 机械硬盘、固态硬盘或任意标签")
	f.allowedOrigins = cmdFiler.Flag.String("allowedOrigins", "*", "逗号分隔的允许来源列表")
	f.exposeDirectoryData = cmdFiler.Flag.Bool("exposeDirectoryData", true, "是否在 Filer UI 中返回目录元数据和内容")
	f.tusBasePath = cmdFiler.Flag.String("tusBasePath", "/.tus", "TUS 可恢复上传端点的基础路径(例如 /.tus)")

	// start s3 on filer
	filerStartS3 = cmdFiler.Flag.Bool("s3", false, "是否启动 S3 网关")
	filerS3Options.port = cmdFiler.Flag.Int("s3.port", 8333, "s3 服务器 http 监听端口")
	filerS3Options.portHttps = cmdFiler.Flag.Int("s3.port.https", 0, "s3 服务器 https 监听端口")
	filerS3Options.portGrpc = cmdFiler.Flag.Int("s3.port.grpc", 0, "s3 服务器 grpc 监听端口")
	filerS3Options.domainName = cmdFiler.Flag.String("s3.domainName", "", "以逗号分隔的主机名后缀列表,{bucket}.{domainName}")
	filerS3Options.allowedOrigins = cmdFiler.Flag.String("s3.allowedOrigins", "*", "逗号分隔的允许来源列表")
	filerS3Options.dataCenter = cmdFiler.Flag.String("s3.dataCenter", "", "优先读写此数据中心中的 volume")
	filerS3Options.tlsPrivateKey = cmdFiler.Flag.String("s3.key.file", "", "TLS 私钥文件路径")
	filerS3Options.tlsCertificate = cmdFiler.Flag.String("s3.cert.file", "", "TLS 证书文件路径")
	filerS3Options.config = cmdFiler.Flag.String("s3.config", "", "配置文件路径")
	filerS3Options.iamConfig = cmdFiler.Flag.String("s3.iam.config", "", "高级 IAM 配置文件路径")
	filerS3Options.auditLogConfig = cmdFiler.Flag.String("s3.auditLogConfig", "", "审计日志配置文件路径")
	filerS3Options.metricsHttpPort = cmdFiler.Flag.Int("s3.metricsPort", 0, "Prometheus 指标监听端口")
	filerS3Options.metricsHttpIp = cmdFiler.Flag.String("s3.metricsIp", "", "指标监听 IP。为空则默认与 -s3.ip.bind 选项相同。")
	cmdFiler.Flag.Bool("s3.allowEmptyFolder", true, "已废弃,忽略。空目录清理现已自动执行。")
	filerS3Options.allowDeleteBucketNotEmpty = cmdFiler.Flag.Bool("s3.allowDeleteBucketNotEmpty", true, "允许随桶一起递归删除所有条目")
	filerS3Options.localSocket = cmdFiler.Flag.String("s3.localSocket", "", "默认为 /tmp/seaweedfs-s3-<port>.sock")
	filerS3Options.tlsCACertificate = cmdFiler.Flag.String("s3.cacert.file", "", "TLS CA 证书文件路径")
	filerS3Options.tlsVerifyClientCert = cmdFiler.Flag.Bool("s3.tlsVerifyClientCert", false, "是否校验客户端证书")
	filerS3Options.bindIp = cmdFiler.Flag.String("s3.ip.bind", "", "绑定的 IP 地址。为空则默认与 -ip.bind 选项相同。")
	filerS3Options.idleTimeout = cmdFiler.Flag.Int("s3.idleTimeout", 120, "连接空闲秒数")
	filerS3Options.concurrentUploadLimitMB = cmdFiler.Flag.Int("s3.concurrentUploadLimitMB", 0, "限制 S3 的总并发上传大小,0 表示不限制")
	filerS3Options.concurrentFileUploadLimit = cmdFiler.Flag.Int("s3.concurrentFileUploadLimit", 0, "限制 S3 的并发文件上传数量,0 表示不限制")
	filerS3Options.enableIam = cmdFiler.Flag.Bool("s3.iam", true, "在同一 S3 端口上启用内嵌 IAM API")
	filerS3Options.cipher = cmdFiler.Flag.Bool("s3.encryptVolumeData", false, "为 S3 上传加密 volume 服务器上的数据")
	filerS3Options.iamReadOnly = cmdFiler.Flag.Bool("s3.iam.readOnly", true, "在此服务器上禁用 IAM 写操作")
	filerS3Options.portIceberg = cmdFiler.Flag.Int("s3.port.iceberg", 8181, "Iceberg REST Catalog 服务器监听端口(0 表示禁用)")
	filerS3Options.externalUrl = cmdFiler.Flag.String("s3.externalUrl", "", "客户端用于连接的外部 URL(例如 https://api.example.com:9000)。用于反向代理后的 S3 签名校验。回退到 S3_EXTERNAL_URL 环境变量。")
	filerS3Options.defaultFileMode = cmdFiler.Flag.String("s3.defaultFileMode", "", "S3 上传对象的默认文件模式,例如 0660、0644、0666")
	filerS3Options.cacheSizeMB = cmdFiler.Flag.Int64("s3.cacheCapacityMB", 0, "in-memory chunk cache capacity in MB for S3 GETs shared across requests (0 disables)")

	// start webdav on filer
	filerStartWebDav = cmdFiler.Flag.Bool("webdav", false, "是否启动 webdav 网关")
	filerWebDavOptions.port = cmdFiler.Flag.Int("webdav.port", 7333, "webdav 服务器 http 监听端口")
	filerWebDavOptions.collection = cmdFiler.Flag.String("webdav.collection", "", "创建文件所用的集合")
	filerWebDavOptions.replication = cmdFiler.Flag.String("webdav.replication", "", "创建文件所用的副本策略")
	filerWebDavOptions.disk = cmdFiler.Flag.String("webdav.disk", "", "[hdd|ssd|<tag>] 机械硬盘、固态硬盘或任意标签")
	filerWebDavOptions.tlsPrivateKey = cmdFiler.Flag.String("webdav.key.file", "", "TLS 私钥文件路径")
	filerWebDavOptions.tlsCertificate = cmdFiler.Flag.String("webdav.cert.file", "", "TLS 证书文件路径")
	filerWebDavOptions.cacheDir = cmdFiler.Flag.String("webdav.cacheDir", os.TempDir(), "文件 chunk 的本地缓存目录")
	filerWebDavOptions.cacheSizeMB = cmdFiler.Flag.Int64("webdav.cacheCapacityMB", 0, "local cache capacity in MB")
	filerWebDavOptions.maxMB = cmdFiler.Flag.Int("webdav.maxMB", 4, "拆分超过该限制的大文件")
	filerWebDavOptions.filerRootPath = cmdFiler.Flag.String("webdav.filer.path", "/", "使用来自 filer 服务器的此远程路径")

	// start iam on filer
	filerStartIam = cmdFiler.Flag.Bool("iam", false, "是否启动 IAM 服务")
	filerIamOptions.ip = cmdFiler.Flag.String("iam.ip", *f.ip, "iam 服务器 http 监听 IP 地址")
	filerIamOptions.port = cmdFiler.Flag.Int("iam.port", 8111, "iam 服务器 http 监听端口")

	filerStartSftp = cmdFiler.Flag.Bool("sftp", false, "是否启动 SFTP 服务器")
	filerSftpOptions.port = cmdFiler.Flag.Int("sftp.port", 2022, "SFTP 服务器监听端口")
	filerSftpOptions.sshPrivateKey = cmdFiler.Flag.String("sftp.sshPrivateKey", "", "用于主机认证的 SSH 私钥文件路径")
	filerSftpOptions.hostKeysFolder = cmdFiler.Flag.String("sftp.hostKeysFolder", "", "包含用于主机认证的 SSH 私钥文件的文件夹路径")
	filerSftpOptions.authMethods = cmdFiler.Flag.String("sftp.authMethods", "password,publickey", "逗号分隔的允许认证方式列表:password、publickey、certificate")
	filerSftpOptions.maxAuthTries = cmdFiler.Flag.Int("sftp.maxAuthTries", 6, "每个连接的最大认证尝试次数")
	filerSftpOptions.bannerMessage = cmdFiler.Flag.String("sftp.bannerMessage", "SeaweedFS SFTP Server - Unauthorized access is prohibited", "认证前显示的消息")
	filerSftpOptions.loginGraceTime = cmdFiler.Flag.Duration("sftp.loginGraceTime", 2*time.Minute, "认证超时")
	filerSftpOptions.clientAliveInterval = cmdFiler.Flag.Duration("sftp.clientAliveInterval", 5*time.Second, "发送保活消息的间隔")
	filerSftpOptions.clientAliveCountMax = cmdFiler.Flag.Int("sftp.clientAliveCountMax", 3, "断开连接前允许错过的最大保活消息数")
	filerSftpOptions.userStoreFile = cmdFiler.Flag.String("sftp.userStoreFile", "", "包含用户凭证和权限的 JSON 文件路径")
	filerSftpOptions.trustedUserCAKeysFile = cmdFiler.Flag.String("sftp.trustedUserCAKeysFile", "", "包含受信任用户 CA 公钥的文件路径(OpenSSH authorized_keys 格式);当 -sftp.authMethods 中包含 'certificate' 时必需")
	filerSftpOptions.dataCenter = cmdFiler.Flag.String("sftp.dataCenter", "", "优先读写此数据中心中的 volume")
	filerSftpOptions.bindIp = cmdFiler.Flag.String("sftp.ip.bind", "", "绑定的 IP 地址。为空则默认与 -ip.bind 选项相同。")
	filerSftpOptions.localSocket = cmdFiler.Flag.String("sftp.localSocket", "", "默认为 /tmp/seaweedfs-sftp-<port>.sock")
}

func filerLongDesc() string {
	desc := `启动一个文件服务器,接受对任意文件的 REST 操作。

	//创建或覆盖文件,/path/to 目录会自动创建
	POST /path/to/file
	//获取文件内容
	GET /path/to/file
	//创建或覆盖文件,使用 multipart 请求中的文件名
	POST /path/to/
	//返回 json 格式的子目录和文件列表
	GET /path/to/

	配置文件 "filer.toml" 会按以下顺序读取:"."、"$HOME/.seaweedfs/"、"/usr/local/etc/seaweedfs/" 或 "/etc/seaweedfs/"。
	如果找不到 "filer.toml",会在 "-defaultStoreDir" 下创建一个内嵌的 filer 存储。

	示例 filer.toml 配置文件可通过 "weed scaffold -config=filer" 生成

支持的 Filer 存储:
`

	storeNames := make([]string, len(filer.Stores))
	for i, store := range filer.Stores {
		storeNames[i] = "\t" + store.GetName()
	}
	sort.Strings(storeNames)
	storeList := strings.Join(storeNames, "\n")
	return desc + storeList
}

var cmdFiler = &Command{
	UsageLine: "filer -port=8888 -master=<ip:port>[,<ip:port>]*",
	Short:     "启动一个指向一个或多个 master 服务器的文件服务器",
	Long:      filerLongDesc(),
}

func runFiler(cmd *Command, args []string) bool {
	if *f.debug {
		go http.ListenAndServe(fmt.Sprintf(":%d", *f.debugPort), nil)
	}

	*f.defaultLevelDbDirectory = util.ResolvePath(*f.defaultLevelDbDirectory)
	filerS3Options.resolvePaths()
	filerWebDavOptions.resolvePaths()
	filerSftpOptions.resolvePaths()
	util.LoadSecurityConfiguration()

	// Share the S3 static identity config file with the filer regardless of
	// whether the embedded S3 gateway runs on this node: the IAM gRPC service
	// the admin UI and weed shell talk to is wired up unconditionally, and it
	// needs the same identities the S3 server would load from -s3.config.
	f.s3ConfigFile = filerS3Options.config

	switch {
	case *f.metricsHttpIp != "":
		// noting to do, use f.metricsHttpIp
	case *f.bindIp != "":
		*f.metricsHttpIp = *f.bindIp
	case *f.ip != "":
		*f.metricsHttpIp = *f.ip
	}
	go stats_collect.StartMetricsServer(*f.metricsHttpIp, *f.metricsHttpPort)

	filerAddress := pb.NewServerAddress(*f.ip, *f.port, *f.portGrpc).String()
	startDelay := time.Duration(2)
	if *filerStartS3 {
		filerS3Options.filer = &filerAddress
		filerS3Options.ip = f.ip
		if *filerS3Options.bindIp == "" {
			filerS3Options.bindIp = f.bindIp
		}
		filerS3Options.localFilerSocket = f.localSocket
		if *f.dataCenter != "" && *filerS3Options.dataCenter == "" {
			filerS3Options.dataCenter = f.dataCenter
		}
		// Set S3 metrics IP based on bind IP if not explicitly set
		if *filerS3Options.metricsHttpIp == "" {
			*filerS3Options.metricsHttpIp = *filerS3Options.bindIp
		}
		go func(delay time.Duration) {
			time.Sleep(delay * time.Second)
			filerS3Options.startS3Server()
		}(startDelay)
		startDelay++
	}

	if *filerStartWebDav {
		filerWebDavOptions.filer = &filerAddress
		filerWebDavOptions.ipBind = f.bindIp

		if *filerWebDavOptions.disk == "" {
			filerWebDavOptions.disk = f.diskType
		}

		go func(delay time.Duration) {
			time.Sleep(delay * time.Second)
			filerWebDavOptions.startWebDav()
		}(startDelay)
		startDelay++
	}

	if *filerStartIam {
		filerIamOptions.filer = &filerAddress
		filerIamOptions.masters = f.mastersString
		go func(delay time.Duration) {
			time.Sleep(delay * time.Second)
			filerIamOptions.startIamServer()
		}(startDelay)
		startDelay++
	}

	if *filerStartSftp {
		filerSftpOptions.filer = &filerAddress
		if *filerSftpOptions.bindIp == "" {
			filerSftpOptions.bindIp = f.bindIp
		}
		if *f.dataCenter != "" && *filerSftpOptions.dataCenter == "" {
			filerSftpOptions.dataCenter = f.dataCenter
		}
		go func(delay time.Duration) {
			time.Sleep(delay * time.Second)
			filerSftpOptions.startSftpServer()
		}(startDelay)
	}

	f.masters = pb.ServerAddresses(*f.mastersString).ToServiceDiscovery()

	f.startFiler()

	return true
}

func (fo *FilerOptions) startFiler() {

	defaultMux := http.NewServeMux()
	publicVolumeMux := defaultMux

	if *fo.publicPort != 0 {
		publicVolumeMux = http.NewServeMux()
	}
	if *fo.portGrpc == 0 {
		*fo.portGrpc = 10000 + *fo.port
	}
	if *fo.bindIp == "" {
		*fo.bindIp = *fo.ip
	}
	util.SetOutboundLocalIP(*fo.bindIp)
	if *fo.allowedOrigins == "" {
		*fo.allowedOrigins = "*"
	}

	defaultLevelDbDirectory := *fo.defaultLevelDbDirectory + "/filerldb2"

	filerAddress := pb.NewServerAddress(*fo.ip, *fo.port, *fo.portGrpc)

	// Initialize credential manager for IAM gRPC service
	var credentialManager *credential.CredentialManager
	var err error
	credentialManager, err = credential.NewCredentialManagerWithDefaults("")
	if err != nil {
		glog.Warningf("Failed to initialize credential manager: %v", err)
	} else {
		glog.V(0).Infof("Initialized credential manager: %s", credentialManager.GetStoreName())
	}

	// Load static S3 identities from config file if specified
	if fo.s3ConfigFile != nil && *fo.s3ConfigFile != "" {
		if credentialManager != nil {
			if err := credentialManager.LoadS3ConfigFile(*fo.s3ConfigFile); err != nil {
				glog.Warningf("Failed to load S3 config file for static identities: %v", err)
			}
		}
	}

	fs, nfs_err := weed_server.NewFilerServer(defaultMux, publicVolumeMux, &weed_server.FilerOption{
		Masters:                   fo.masters,
		FilerGroup:                *fo.filerGroup,
		Collection:                *fo.collection,
		DefaultReplication:        *fo.defaultReplicaPlacement,
		DisableDirListing:         *fo.disableDirListing,
		MaxMB:                     *fo.maxMB,
		DirListingLimit:           *fo.dirListingLimit,
		DataCenter:                *fo.dataCenter,
		Rack:                      *fo.rack,
		DefaultLevelDbDir:         defaultLevelDbDirectory,
		DisableHttp:               *fo.disableHttp,
		Host:                      filerAddress,
		Cipher:                    *fo.cipher,
		SaveToFilerLimit:          int64(*fo.saveToFilerLimit),
		ConcurrentUploadLimit:     int64(*fo.concurrentUploadLimitMB) * 1024 * 1024,
		ConcurrentFileUploadLimit: int64(*fo.concurrentFileUploadLimit),
		ShowUIDirectoryDelete:     *fo.showUIDirectoryDelete,
		DownloadMaxBytesPs:        int64(*fo.downloadMaxMBps) * 1024 * 1024,
		DiskType:                  *fo.diskType,
		AllowedOrigins:            strings.Split(*fo.allowedOrigins, ","),
		TusBasePath:               *fo.tusBasePath,
		CredentialManager:         credentialManager,
	})
	if nfs_err != nil {
		glog.Fatalf("Filer startup error: %v", nfs_err)
	}

	// Ensure fs.Shutdown() runs exactly once, whether triggered by a signal hook
	// or by the main goroutine after Serve() returns (e.g., MiniCluster tests).
	var shutdownOnce sync.Once
	shutdownFiler := func() {
		shutdownOnce.Do(func() {
			fs.Shutdown()
		})
	}

	if *fo.publicPort != 0 {
		publicListeningAddress := util.JoinHostPort(*fo.bindIp, *fo.publicPort)
		glog.V(0).Infoln("Start Seaweed filer server", version.Version(), "public at", publicListeningAddress)
		publicListener, localPublicListener, e := util.NewIpAndLocalListeners(*fo.bindIp, *fo.publicPort, 0)
		if e != nil {
			glog.Fatalf("Filer server public listener error on port %d:%v", *fo.publicPort, e)
		}
		go func() {
			if e := http.Serve(publicListener, publicVolumeMux); e != nil {
				glog.Fatalf("Volume server fail to serve public: %v", e)
			}
		}()
		if localPublicListener != nil {
			go func() {
				if e := http.Serve(localPublicListener, publicVolumeMux); e != nil {
					glog.Errorf("Volume server fail to serve public: %v", e)
				}
			}()
		}
	}

	glog.V(0).Infof("Start Seaweed Filer %s at %s:%d", version.Version(), *fo.ip, *fo.port)
	filerListener, filerLocalListener, e := util.NewIpAndLocalListeners(
		*fo.bindIp, *fo.port,
		time.Duration(10)*time.Second,
	)
	if e != nil {
		glog.Fatalf("Filer listener error: %v", e)
	}

	// starting grpc server
	grpcPort := *fo.portGrpc
	grpcL, grpcLocalL, err := util.NewIpAndLocalListeners(*fo.bindIp, grpcPort, 0)
	if err != nil {
		glog.Fatalf("failed to listen on grpc port %d: %v", grpcPort, err)
	}
	grpcS := pb.NewGrpcServer(security.LoadServerTLS(util.GetViper(), "grpc.filer"))
	filer_pb.RegisterSeaweedFilerServer(grpcS, fs)

	// Register the IAM gRPC service. Auth is opt-in: when
	// jwt.filer_signing.key is configured the service requires a Bearer token
	// signed with that key; otherwise it runs unauthenticated, matching the
	// rest of the filer's gRPC surface. Operators who expose the filer gRPC
	// port beyond a trusted network should set jwt.filer_signing.key on both
	// the filer and the admin server.
	if credentialManager != nil {
		adminSigningKey := security.SigningKey(util.GetViper().GetString("jwt.filer_signing.key"))
		iamGrpcServer := weed_server.NewIamGrpcServer(credentialManager, adminSigningKey)
		iam_pb.RegisterSeaweedIdentityAccessManagementServer(grpcS, iamGrpcServer)
		if len(adminSigningKey) == 0 {
			glog.V(0).Info("Registered IAM gRPC service on filer (unauthenticated; set jwt.filer_signing.key in security.toml to require admin Bearer token)")
		} else {
			glog.V(0).Info("Registered IAM gRPC service on filer (admin Bearer token required)")
		}
	}

	reflection.Register(grpcS)
	if grpcLocalL != nil {
		go grpcS.Serve(grpcLocalL)
	}
	go grpcS.Serve(grpcL)
	pb.ServeGrpcOnLocalSocket(grpcS, grpcPort)

	// Helper to gracefully stop the gRPC server, waiting for active RPCs.
	gracefulTimeout := fo.gracefulStopTimeout
	if gracefulTimeout <= 0 {
		gracefulTimeout = 10 * time.Second
	}
	stopGrpcServer := func() {
		glog.V(0).Infof("Gracefully stopping gRPC server")
		stopped := make(chan struct{})
		go func() {
			grpcS.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
			glog.V(0).Infof("gRPC server stopped gracefully")
		case <-time.After(gracefulTimeout):
			glog.V(0).Infof("gRPC server graceful stop timed out after %s, forcing stop", gracefulTimeout)
			grpcS.Stop()
		}
	}

	var socketServer *http.Server
	if runtime.GOOS != "windows" {
		localSocket := *fo.localSocket
		if localSocket == "" {
			localSocket = fmt.Sprintf("/tmp/seaweedfs-filer-%d.sock", *fo.port)
		}
		if err := os.Remove(localSocket); err != nil && !os.IsNotExist(err) {
			glog.Fatalf("Failed to remove %s, error: %s", localSocket, err.Error())
		}
		filerSocketListener, err := net.Listen("unix", localSocket)
		if err != nil {
			glog.Fatalf("Failed to listen on %s: %v", localSocket, err)
		}
		socketServer = newHttpServer(defaultMux, nil)
		go socketServer.Serve(filerSocketListener)
	}

	if viper.GetString("https.filer.key") != "" {
		certFile := viper.GetString("https.filer.cert")
		keyFile := viper.GetString("https.filer.key")
		caCertFile := viper.GetString("https.filer.ca")
		disbaleTlsVerifyClientCert := viper.GetBool("https.filer.disable_tls_verify_client_cert")

		getCert, certProvider, err := security.NewReloadingServerCertificate(certFile, keyFile)
		if err != nil {
			glog.Fatalf("Filer failed to load HTTPS certificate: %v", err)
		}
		defer certProvider.Close()

		caCertPool := x509.NewCertPool()
		if caCertFile != "" {
			caCertFile, err := os.ReadFile(caCertFile)
			if err != nil {
				glog.Fatalf("error reading CA certificate: %v", err)
			}
			caCertPool.AppendCertsFromPEM(caCertFile)
		}

		clientAuth := tls.NoClientCert
		if !disbaleTlsVerifyClientCert {
			clientAuth = tls.RequireAndVerifyClientCert
		}

		tlsConfig := &tls.Config{
			GetCertificate: getCert,
			ClientAuth:     clientAuth,
			ClientCAs:      caCertPool,
		}

		err = security.FixTlsConfig(util.GetViper(), tlsConfig)
		if err != nil {
			glog.Fatalf("Filer failed to fix TLS config: %v", err)
		}

		var localTLSServer *http.Server
		if filerLocalListener != nil {
			localTLSServer = newHttpServer(defaultMux, tlsConfig)
			go func() {
				if err := localTLSServer.ServeTLS(filerLocalListener, "", ""); err != nil {
					glog.Errorf("Filer Fail to serve: %v", err)
				}
			}()
		}
		httpS := newHttpServer(defaultMux, tlsConfig)

		// Register a single shutdown hook that runs the steps in the correct order:
		// stop accepting new gRPC/HTTP requests, then close the filer database.
		// Combining them into one hook keeps ordering intact regardless of how
		// grace fires interrupt hooks (FIFO vs LIFO).
		grace.OnInterrupt(func() {
			stopGrpcServer()
			glog.V(0).Infof("Gracefully stopping all HTTP servers")
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if socketServer != nil {
				err = socketServer.Shutdown(shutdownCtx)
				if err != nil {
					glog.Warningf("socket server shutdown: %v", err)
				}
			}
			if localTLSServer != nil {
				err = localTLSServer.Shutdown(shutdownCtx)
				if err != nil {
					glog.Warningf("local TLS server shutdown: %v", err)
				}
			}
			if err := httpS.Shutdown(shutdownCtx); err != nil {
				glog.Warningf("HTTPS server shutdown: %v", err)
			}
			shutdownFiler()
		})

		if fo.shutdownCtx != nil {
			go func() {
				<-fo.shutdownCtx.Done()
				httpS.Shutdown(context.Background())
				grpcS.Stop()
			}()
		}
		if err := httpS.ServeTLS(filerListener, "", ""); err != nil && err != http.ErrServerClosed {
			glog.Fatalf("Filer Fail to serve: %v", err)
		}
		// Close database after servers have stopped to prevent data corruption
		shutdownFiler()
	} else {
		var localHTTPServer *http.Server
		if filerLocalListener != nil {
			localHTTPServer = newHttpServer(defaultMux, nil)
			go func() {
				if err := localHTTPServer.Serve(filerLocalListener); err != nil {
					glog.Errorf("Filer Fail to serve: %v", err)
				}
			}()
		}
		httpS := newHttpServer(defaultMux, nil)

		// Register a single shutdown hook that runs the steps in the correct order:
		// stop accepting new gRPC/HTTP requests, then close the filer database.
		// Combining them into one hook keeps ordering intact regardless of how
		// grace fires interrupt hooks (FIFO vs LIFO).
		grace.OnInterrupt(func() {
			stopGrpcServer()
			glog.V(0).Infof("Gracefully stopping all HTTP servers")
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if socketServer != nil {
				socketServer.Shutdown(shutdownCtx)
			}
			if localHTTPServer != nil {
				localHTTPServer.Shutdown(shutdownCtx)
			}
			if err := httpS.Shutdown(shutdownCtx); err != nil {
				glog.Warningf("HTTP server shutdown: %v", err)
			}
			shutdownFiler()
		})

		if fo.shutdownCtx != nil {
			go func() {
				<-fo.shutdownCtx.Done()
				httpS.Shutdown(context.Background())
				grpcS.Stop()
			}()
		}
		if err := httpS.Serve(filerListener); err != nil && err != http.ErrServerClosed {
			glog.Fatalf("Filer Fail to serve: %v", err)
		}
		// Close database after servers have stopped to prevent data corruption
		shutdownFiler()
	}
}
