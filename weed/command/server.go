package command

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"
)

type ServerOptions struct {
	cpuprofile *string
	memprofile *string
	debug      *bool
	debugPort  *int
	v          VolumeServerOptions
}

var (
	serverOptions        ServerOptions
	masterOptions        MasterOptions
	filerOptions         FilerOptions
	s3Options            S3Options
	sftpOptions          SftpOptions
	iamOptions           IamOptions
	webdavOptions        WebDavOption
	mqBrokerOptions      MessageQueueBrokerOptions
	mqAgentServerOptions MessageQueueAgentOptions
)

func init() {
	cmdServer.Run = runServer // break init cycle
}

var cmdServer = &Command{
	UsageLine: "server -dir=/tmp -volume.max=5 -ip=server_name",
	Short:     "启动 master 服务器、volume 服务器,可选启动 filer 和 S3 网关",
	Long: `同时启动一个提供存储空间的 volume 服务器
  和一个提供 volume=>location 映射服务及文件 ID 序列号的 master 服务器

  这是一种同时启动 volume 服务器和 master 服务器的便捷方式。
  这些服务器的行为与分别启动它们完全相同。
  因此其他 volume 服务器也可以连接到这个 master 服务器。

  可选地,可以启动一个 filer 服务器。
  也可选地,可以启动一个 S3 网关。

  `,
}

var (
	serverIp                  = cmdServer.Flag.String("ip", util.DetectedHostAddress(), "IP 或服务器名,同时用作标识符")
	serverBindIp              = cmdServer.Flag.String("ip.bind", "", "绑定的 IP 地址。为空则默认与 -ip 选项相同。")
	serverTimeout             = cmdServer.Flag.Int("idleTimeout", 30, "连接空闲秒数")
	serverDataCenter          = cmdServer.Flag.String("dataCenter", "", "当前 volume 服务器的数据中心名称")
	serverRack                = cmdServer.Flag.String("rack", "", "当前 volume 服务器的机架名称")
	serverWhiteListOption     = cmdServer.Flag.String("whiteList", "", "逗号分隔的具有写权限的 IP 地址列表,为空则不限制")
	serverDisableHttp         = cmdServer.Flag.Bool("disableHttp", false, "禁用 http 请求,仅允许 gRPC 操作。")
	serverIamConfig           = cmdServer.Flag.String("iam.config", "", "S3 的高级 IAM 配置文件路径。是 -s3.iam.config 的别名,但优先级更低。")
	volumeDataFolders         = cmdServer.Flag.String("dir", os.TempDir(), "存储数据文件的目录。dir[,dir]...")
	volumeMaxDataVolumeCounts = cmdServer.Flag.String("volume.max", "8", "最大 volume 数量,count[,count]... 设为 0 时,将按可用磁盘空间除以 volume 大小自动配置限制。")
	volumeMinFreeSpacePercent = cmdServer.Flag.String("volume.minFreeSpacePercent", "1", "最小可用磁盘空间(默认为 1%)。磁盘空间不足时,所有 volume 将被标记为 ReadOnly(已废弃,请改用 minFreeSpace)。")
	volumeMinFreeSpace        = cmdServer.Flag.String("volume.minFreeSpace", "", "最小可用磁盘空间(value<=100 时为百分比如 1,其他为人类可读的字节数,如 10GiB)。磁盘空间不足时,所有 volume 将被标记为 ReadOnly。")
	serverMetricsHttpPort     = cmdServer.Flag.Int("metricsPort", 0, "Prometheus 指标监听端口")
	serverMetricsHttpIp       = cmdServer.Flag.String("metricsIp", "", "指标监听 IP。为空则默认与 -ip.bind 选项相同。")

	// pulseSeconds              = cmdServer.Flag.Int("pulseSeconds", 5, "心跳之间的秒数")
	isStartingMasterServer = cmdServer.Flag.Bool("master", true, "是否启动 master 服务器")
	isStartingVolumeServer = cmdServer.Flag.Bool("volume", true, "是否启动 volume 服务器")
	isStartingFiler        = cmdServer.Flag.Bool("filer", false, "是否启动 filer")
	isStartingS3           = cmdServer.Flag.Bool("s3", false, "是否启动 S3 网关")
	isStartingSftp         = cmdServer.Flag.Bool("sftp", false, "是否启动 Sftp 服务器")
	isStartingIam          = cmdServer.Flag.Bool("iam", false, "是否启动 IAM 服务")
	isStartingWebDav       = cmdServer.Flag.Bool("webdav", false, "是否启动 WebDAV 网关")
	isStartingMqBroker     = cmdServer.Flag.Bool("mq.broker", false, "是否启动消息队列 broker")
	isStartingMqAgent      = cmdServer.Flag.Bool("mq.agent", false, "是否启动消息队列 agent")

	False = false
)

func init() {
	serverOptions.cpuprofile = cmdServer.Flag.String("cpuprofile", "", "cpu profile 输出文件")
	serverOptions.memprofile = cmdServer.Flag.String("memprofile", "", "memory profile 输出文件")
	serverOptions.debug = cmdServer.Flag.Bool("debug", false, "提供运行时性能分析数据,例如 http://localhost:6060/debug/pprof/goroutine?debug=2")
	serverOptions.debugPort = cmdServer.Flag.Int("debug.port", 6060, "用于调试的 http 端口")

	masterOptions.port = cmdServer.Flag.Int("master.port", 9333, "master 服务器 http 监听端口")
	masterOptions.portGrpc = cmdServer.Flag.Int("master.port.grpc", 0, "master 服务器 grpc 监听端口")
	masterOptions.metaFolder = cmdServer.Flag.String("master.dir", "", "存储元数据的数据目录,默认与 -dir 指定的相同")
	masterOptions.peers = cmdServer.Flag.String("master.peers", "", "以逗号分隔的 ip:masterPort 列表形式给出所有 master 节点")
	masterOptions.volumeSizeLimitMB = cmdServer.Flag.Uint("master.volumeSizeLimitMB", 30*1000, "master 停止向超限 volume 分配写入")
	masterOptions.volumePreallocate = cmdServer.Flag.Bool("master.volumePreallocate", false, "为 volume 预分配磁盘空间。")
	masterOptions.maxParallelVacuumPerServer = cmdServer.Flag.Int("master.maxParallelVacuumPerServer", 1, "每个 volume 服务器并行 vacuum 的最大 volume 数量")
	masterOptions.defaultReplication = cmdServer.Flag.String("master.defaultReplication", "", "未指定时的默认副本策略。")
	masterOptions.garbageThreshold = cmdServer.Flag.Float64("master.garbageThreshold", 0.3, "vacuum 并回收空间的阈值")
	masterOptions.metricsAddress = cmdServer.Flag.String("master.metrics.address", "", "Prometheus 网关地址")
	masterOptions.metricsIntervalSec = cmdServer.Flag.Int("master.metrics.intervalSeconds", 15, "Prometheus 推送间隔秒数")
	masterOptions.raftResumeState = cmdServer.Flag.Bool("master.resumeState", true, "启动 master 服务器时恢复之前的状态")
	masterOptions.raftHashicorp = cmdServer.Flag.Bool("master.raftHashicorp", false, "使用 hashicorp raft")
	masterOptions.raftBootstrap = cmdServer.Flag.Bool("master.raftBootstrap", false, "是否引导启动 Raft 集群")
	masterOptions.heartbeatInterval = cmdServer.Flag.Duration("master.heartbeatInterval", 300*time.Millisecond, "master 服务器的心跳间隔,将被随机乘以 [1, 1.25)")
	masterOptions.electionTimeout = cmdServer.Flag.Duration("master.electionTimeout", 10*time.Second, "master 服务器的选举超时时间")
	masterOptions.telemetryUrl = cmdServer.Flag.String("master.telemetry.url", "https://telemetry.seaweedfs.com/api/collect", "用于发送使用统计的遥测服务器 URL")
	masterOptions.telemetryEnabled = cmdServer.Flag.Bool("master.telemetry", true, "向 master.telemetry.url 报告匿名集群统计信息,使用 -master.telemetry=false 可退出")

	filerOptions.filerGroup = cmdServer.Flag.String("filer.filerGroup", "", "与同一 filerGroup 中的其他 filer 共享元数据")
	filerOptions.collection = cmdServer.Flag.String("filer.collection", "", "所有数据将存储在此集合中")
	filerOptions.port = cmdServer.Flag.Int("filer.port", 8888, "filer 服务器 http 监听端口")
	filerOptions.portGrpc = cmdServer.Flag.Int("filer.port.grpc", 0, "filer 服务器 grpc 监听端口")
	filerOptions.publicPort = cmdServer.Flag.Int("filer.port.public", 0, "filer 服务器公共 http 监听端口")
	filerOptions.allowedOrigins = cmdServer.Flag.String("filer.allowedOrigins", "*", "逗号分隔的允许来源列表")
	filerOptions.defaultReplicaPlacement = cmdServer.Flag.String("filer.defaultReplicaPlacement", "", "默认副本策略。未指定时使用 master 设置。")
	filerOptions.disableDirListing = cmdServer.Flag.Bool("filer.disableDirListing", false, "关闭目录列表")
	filerOptions.maxMB = cmdServer.Flag.Int("filer.maxMB", 4, "拆分超过该限制的大文件")
	filerOptions.dirListingLimit = cmdServer.Flag.Int("filer.dirListLimit", 1000, "限制子目录列表大小")
	filerOptions.cipher = cmdServer.Flag.Bool("filer.encryptVolumeData", false, "加密 volume 服务器上的数据")
	filerOptions.saveToFilerLimit = cmdServer.Flag.Int("filer.saveToFilerLimit", 0, "小于此限制的小文件可被缓存到 filer 存储中。")
	filerOptions.concurrentUploadLimitMB = cmdServer.Flag.Int("filer.concurrentUploadLimitMB", 0, "限制总并发上传大小,0 表示不限制")
	filerOptions.concurrentFileUploadLimit = cmdServer.Flag.Int("filer.concurrentFileUploadLimit", 0, "限制并发文件上传数量,0 表示不限制")
	filerOptions.localSocket = cmdServer.Flag.String("filer.localSocket", "", "默认为 /tmp/seaweedfs-filer-<port>.sock")
	filerOptions.showUIDirectoryDelete = cmdServer.Flag.Bool("filer.ui.deleteDir", true, "启用 filer UI 显示删除目录按钮")
	filerOptions.downloadMaxMBps = cmdServer.Flag.Int("filer.downloadMaxMBps", 0, "每个下载请求的最大下载速度,单位为 MB/秒")
	filerOptions.diskType = cmdServer.Flag.String("filer.disk", "", "[hdd|ssd|<tag>] 硬盘或固态硬盘或任意标签")
	filerOptions.exposeDirectoryData = cmdServer.Flag.Bool("filer.exposeDirectoryData", true, "通过 filer 暴露目录数据。如果为 false,filer UI 将无法访问。")
	filerOptions.tusBasePath = cmdServer.Flag.String("filer.tusBasePath", "/.tus", "TUS 可恢复上传端点的基础路径(例如 /.tus)")

	serverOptions.v.port = cmdServer.Flag.Int("volume.port", 8080, "volume 服务器 http 监听端口")
	serverOptions.v.portGrpc = cmdServer.Flag.Int("volume.port.grpc", 0, "volume 服务器 grpc 监听端口")
	serverOptions.v.publicPort = cmdServer.Flag.Int("volume.port.public", 0, "volume 服务器公共端口")
	serverOptions.v.id = cmdServer.Flag.String("volume.id", "", "volume 服务器 ID。为空则默认为 ip:port")
	serverOptions.v.indexType = cmdServer.Flag.String("volume.index", "memory", "选择 [memory|leveldb|leveldbMedium|leveldbLarge] 模式以平衡内存与性能。")
	serverOptions.v.diskType = cmdServer.Flag.String("volume.disk", "", "[hdd|ssd|<tag>] 硬盘或固态硬盘或任意标签")
	serverOptions.v.tags = cmdServer.Flag.String("volume.tags", "", "每个数据目录的逗号分隔标签组;每组使用 ':'(例如 fast:ssd,archive)")
	serverOptions.v.fixJpgOrientation = cmdServer.Flag.Bool("volume.images.fix.orientation", false, "上传时调整 jpg 方向。")
	serverOptions.v.readMode = cmdServer.Flag.String("volume.readMode", "proxy", "[local|proxy|redirect] 如何处理非本地 volume:'未找到|在远程节点读取|重定向 volume 位置'。")
	serverOptions.v.compactionMBPerSecond = cmdServer.Flag.Int("volume.compactionMBps", 0, "限制 compaction 速度,单位为 MB/秒")
	serverOptions.v.maintenanceMBPerSecond = cmdServer.Flag.Int("volume.maintenanceMBps", 0, "限制维护(副本/均衡)IO 速率,单位为 MB/s。未设置为 0,表示不限制。")
	serverOptions.v.fileSizeLimitMB = cmdServer.Flag.Int("volume.fileSizeLimitMB", 256, "限制文件大小以避免内存不足")
	serverOptions.v.ldbTimeout = cmdServer.Flag.Int64("volume.index.leveldbTimeout", 0, "alive time for leveldb (default to 0). If leveldb of volume is not accessed in ldbTimeout hours, it will be off loaded to reduce opened files and memory consumption.")
	serverOptions.v.concurrentUploadLimitMB = cmdServer.Flag.Int("volume.concurrentUploadLimitMB", 0, "限制总并发上传大小,0 表示不限制")
	serverOptions.v.concurrentDownloadLimitMB = cmdServer.Flag.Int("volume.concurrentDownloadLimitMB", 0, "限制总并发下载大小,0 表示不限制")
	serverOptions.v.publicUrl = cmdServer.Flag.String("volume.publicUrl", "", "可公开访问的地址")
	serverOptions.v.preStopSeconds = cmdServer.Flag.Int("volume.preStopSeconds", 10, "停止发送心跳与停止 volume 服务器之间的秒数")
	serverOptions.v.pprof = cmdServer.Flag.Bool("volume.pprof", false, "启用 pprof http 处理程序。与 -memprofile 和 -cpuprofile 互斥")
	serverOptions.v.idxFolder = cmdServer.Flag.String("volume.dir.idx", "", "存储 .idx 文件的目录")
	serverOptions.v.inflightUploadDataTimeout = cmdServer.Flag.Duration("volume.inflightUploadDataTimeout", 60*time.Second, "volume 服务器的进行中上传数据等待超时")
	serverOptions.v.inflightDownloadDataTimeout = cmdServer.Flag.Duration("volume.inflightDownloadDataTimeout", 60*time.Second, "volume 服务器的进行中下载数据等待超时")

	serverOptions.v.hasSlowRead = cmdServer.Flag.Bool("volume.hasSlowRead", true, "<experimental> 如果为 true,可防止慢读取阻塞其他请求,但大文件读取 P99 延迟将增加。")
	serverOptions.v.readBufferSizeMB = cmdServer.Flag.Int("volume.readBufferSizeMB", 4, "<experimental> 较大的值可优化查询性能,但会增加部分内存使用,通常与 hasSlowRead 一起使用")
	serverOptions.v.allowUntrustedRemoteEndpoints = cmdServer.Flag.Bool("volume.allowUntrustedRemoteEndpoints", false, "如果为 true,FetchAndWriteNeedle 将接受任意远程 S3 端点,包括回环/链路本地主机。默认拒绝内部/元数据端点。")
	serverOptions.v.setDiskIOProbeDefaults()

	s3Options.port = cmdServer.Flag.Int("s3.port", 8333, "s3 服务器 http 监听端口")
	s3Options.portHttps = cmdServer.Flag.Int("s3.port.https", 0, "s3 服务器 https 监听端口")
	s3Options.portGrpc = cmdServer.Flag.Int("s3.port.grpc", 0, "s3 服务器 grpc 监听端口")
	s3Options.portIceberg = cmdServer.Flag.Int("s3.port.iceberg", 8181, "Iceberg REST Catalog 服务器监听端口(0 表示禁用)")
	s3Options.domainName = cmdServer.Flag.String("s3.domainName", "", "以逗号分隔的主机名后缀列表,{bucket}.{domainName}")
	s3Options.allowedOrigins = cmdServer.Flag.String("s3.allowedOrigins", "*", "逗号分隔的允许来源列表")
	s3Options.tlsPrivateKey = cmdServer.Flag.String("s3.key.file", "", "TLS 私钥文件路径")
	s3Options.tlsCertificate = cmdServer.Flag.String("s3.cert.file", "", "TLS 证书文件路径")
	s3Options.tlsCACertificate = cmdServer.Flag.String("s3.cacert.file", "", "TLS CA 证书文件路径")
	s3Options.tlsVerifyClientCert = cmdServer.Flag.Bool("s3.tlsVerifyClientCert", false, "是否验证客户端证书")
	s3Options.config = cmdServer.Flag.String("s3.config", "", "配置文件路径")
	s3Options.iamConfig = cmdServer.Flag.String("s3.iam.config", "", "S3 的高级 IAM 配置文件路径。如果同时提供 -iam.config,则覆盖 -iam.config。")
	s3Options.auditLogConfig = cmdServer.Flag.String("s3.auditLogConfig", "", "审计日志配置文件路径")
	cmdServer.Flag.Bool("s3.allowEmptyFolder", true, "已废弃,忽略。空文件夹清理现已自动执行。")
	s3Options.allowDeleteBucketNotEmpty = cmdServer.Flag.Bool("s3.allowDeleteBucketNotEmpty", true, "允许连同 bucket 递归删除所有条目")
	s3Options.localSocket = cmdServer.Flag.String("s3.localSocket", "", "默认为 /tmp/seaweedfs-s3-<port>.sock")
	s3Options.bindIp = cmdServer.Flag.String("s3.ip.bind", "", "绑定的 IP 地址。为空则默认与 -ip.bind 选项相同。")
	s3Options.idleTimeout = cmdServer.Flag.Int("s3.idleTimeout", 120, "连接空闲秒数")
	s3Options.concurrentUploadLimitMB = cmdServer.Flag.Int("s3.concurrentUploadLimitMB", 0, "限制 S3 的总并发上传大小,0 表示不限制")
	s3Options.concurrentFileUploadLimit = cmdServer.Flag.Int("s3.concurrentFileUploadLimit", 0, "限制 S3 的并发文件上传数量,0 表示不限制")
	s3Options.enableIam = cmdServer.Flag.Bool("s3.iam", true, "在同一 S3 端口上启用内嵌 IAM API")
	s3Options.iamReadOnly = cmdServer.Flag.Bool("s3.iam.readOnly", true, "禁用此服务器上的 IAM 写操作")
	s3Options.cipher = cmdServer.Flag.Bool("s3.encryptVolumeData", false, "为 S3 上传加密 volume 服务器上的数据")
	s3Options.externalUrl = cmdServer.Flag.String("s3.externalUrl", "", "客户端用于连接的外部 URL(例如 https://api.example.com:9000)。用于反向代理后的 S3 签名验证。回退到 S3_EXTERNAL_URL 环境变量。")
	s3Options.defaultFileMode = cmdServer.Flag.String("s3.defaultFileMode", "", "S3 上传对象的默认文件模式,例如 0660、0644、0666")
	s3Options.cacheSizeMB = cmdServer.Flag.Int64("s3.cacheCapacityMB", 0, "in-memory chunk cache capacity in MB for S3 GETs shared across requests (0 disables)")

	sftpOptions.port = cmdServer.Flag.Int("sftp.port", 2022, "SFTP 服务器监听端口")
	sftpOptions.sshPrivateKey = cmdServer.Flag.String("sftp.sshPrivateKey", "", "用于主机认证的 SSH 私钥文件路径")
	sftpOptions.hostKeysFolder = cmdServer.Flag.String("sftp.hostKeysFolder", "", "包含用于主机认证的 SSH 私钥文件的文件夹路径")
	sftpOptions.authMethods = cmdServer.Flag.String("sftp.authMethods", "password,publickey", "逗号分隔的允许认证方法列表:password、publickey、certificate")
	sftpOptions.maxAuthTries = cmdServer.Flag.Int("sftp.maxAuthTries", 6, "每个连接的最大认证尝试次数")
	sftpOptions.bannerMessage = cmdServer.Flag.String("sftp.bannerMessage", "SeaweedFS SFTP Server - Unauthorized access is prohibited", "认证前显示的消息")
	sftpOptions.loginGraceTime = cmdServer.Flag.Duration("sftp.loginGraceTime", 2*time.Minute, "认证超时时间")
	sftpOptions.clientAliveInterval = cmdServer.Flag.Duration("sftp.clientAliveInterval", 5*time.Second, "发送 keep-alive 消息的间隔")
	sftpOptions.clientAliveCountMax = cmdServer.Flag.Int("sftp.clientAliveCountMax", 3, "断开连接前允许错过的最大 keep-alive 消息数")
	sftpOptions.userStoreFile = cmdServer.Flag.String("sftp.userStoreFile", "", "包含用户凭证和权限的 JSON 文件路径")
	sftpOptions.trustedUserCAKeysFile = cmdServer.Flag.String("sftp.trustedUserCAKeysFile", "", "包含受信任用户 CA 公钥的文件路径(OpenSSH authorized_keys 格式);当 -sftp.authMethods 中包含 'certificate' 时必需")
	sftpOptions.localSocket = cmdServer.Flag.String("sftp.localSocket", "", "默认为 /tmp/seaweedfs-sftp-<port>.sock")
	iamOptions.port = cmdServer.Flag.Int("iam.port", 8111, "iam 服务器 http 监听端口")

	webdavOptions.port = cmdServer.Flag.Int("webdav.port", 7333, "webdav 服务器 http 监听端口")
	webdavOptions.collection = cmdServer.Flag.String("webdav.collection", "", "用于创建文件的集合")
	webdavOptions.replication = cmdServer.Flag.String("webdav.replication", "", "用于创建文件的副本策略")
	webdavOptions.disk = cmdServer.Flag.String("webdav.disk", "", "[hdd|ssd|<tag>] 硬盘或固态硬盘或任意标签")
	webdavOptions.tlsPrivateKey = cmdServer.Flag.String("webdav.key.file", "", "TLS 私钥文件路径")
	webdavOptions.tlsCertificate = cmdServer.Flag.String("webdav.cert.file", "", "TLS 证书文件路径")
	webdavOptions.cacheDir = cmdServer.Flag.String("webdav.cacheDir", os.TempDir(), "用于文件分片的本地缓存目录")
	webdavOptions.maxMB = cmdServer.Flag.Int("webdav.maxMB", 4, "拆分超过该限制的大文件")
	webdavOptions.filerRootPath = cmdServer.Flag.String("webdav.filer.path", "/", "使用来自 filer 服务器的此远程路径")

	mqBrokerOptions.port = cmdServer.Flag.Int("mq.broker.port", 17777, "消息队列 broker 的 gRPC 监听端口")
	mqBrokerOptions.logFlushInterval = cmdServer.Flag.Int("mq.broker.logFlushInterval", 5, "日志缓冲区刷新间隔秒数")

	mqAgentServerOptions.brokersString = cmdServer.Flag.String("mq.agent.brokers", "localhost:17777", "逗号分隔的消息队列 broker")
	mqAgentServerOptions.port = cmdServer.Flag.Int("mq.agent.port", 16777, "消息队列 agent 的 gRPC 监听端口")

}

func runServer(cmd *Command, args []string) bool {

	if *serverOptions.debug {
		go http.ListenAndServe(fmt.Sprintf(":%d", *serverOptions.debugPort), nil)
	}

	util.LoadSecurityConfiguration()
	util.LoadConfiguration("master", false)
	util.LoadConfiguration("volume", false)
	serverOptions.v.applyDiskIOProbeConfig()

	*serverOptions.cpuprofile = util.ResolvePath(*serverOptions.cpuprofile)
	*serverOptions.memprofile = util.ResolvePath(*serverOptions.memprofile)
	*serverIamConfig = util.ResolvePath(*serverIamConfig)
	*masterOptions.metaFolder = util.ResolvePath(*masterOptions.metaFolder)
	s3Options.resolvePaths()
	webdavOptions.resolvePaths()
	sftpOptions.resolvePaths()
	grace.SetupProfiling(*serverOptions.cpuprofile, *serverOptions.memprofile)

	if *isStartingS3 {
		*isStartingFiler = true
	}
	if *isStartingSftp {
		*isStartingFiler = true
	}
	if *isStartingIam {
		*isStartingFiler = true
	}
	if *isStartingWebDav {
		*isStartingFiler = true
	}
	if *isStartingMqBroker {
		*isStartingFiler = true
	}
	if *isStartingMqAgent {
		*isStartingMqBroker = true
		*isStartingFiler = true
	}

	var actualPeersForComponents string
	if *isStartingMasterServer {
		// If we are starting a master, validate and complete the peer list
		_, peerList := checkPeers(*serverIp, *masterOptions.port, *masterOptions.portGrpc, *masterOptions.peers)
		actualPeersForComponents = strings.Join(pb.ToAddressStrings(peerList), ",")
	} else if *masterOptions.peers != "" {
		if isSingleMasterMode(*masterOptions.peers) {
			glog.Fatalf("'-master.peers=none' is only valid when starting a master server, but master is not starting.")
		}
		// If not starting a master, just use the provided peers
		actualPeersForComponents = *masterOptions.peers
	}

	if *serverBindIp == "" {
		serverBindIp = serverIp
	}
	// Bind outbound connections to the same address up front so every
	// component started below inherits it, before any of them dials.
	util.SetOutboundLocalIP(*serverBindIp)

	if *serverMetricsHttpIp == "" {
		*serverMetricsHttpIp = *serverBindIp
	}

	// ip address
	masterOptions.ip = serverIp
	masterOptions.ipBind = serverBindIp
	// Use actualPeersForComponents for volume/filer, not masterOptions.peers which might be "none"
	filerOptions.masters = pb.ServerAddresses(actualPeersForComponents).ToServiceDiscovery()
	filerOptions.ip = serverIp
	filerOptions.bindIp = serverBindIp
	s3Options.ip = serverIp
	if *s3Options.bindIp == "" {
		s3Options.bindIp = serverBindIp
	}
	if sftpOptions.bindIp == nil || *sftpOptions.bindIp == "" {
		sftpOptions.bindIp = serverBindIp
	}
	iamOptions.ip = serverBindIp
	iamOptions.masters = &actualPeersForComponents
	webdavOptions.ipBind = serverBindIp
	serverOptions.v.ip = serverIp
	serverOptions.v.bindIp = serverBindIp
	serverOptions.v.masters = pb.ServerAddresses(actualPeersForComponents).ToAddresses()
	serverOptions.v.idleConnectionTimeout = serverTimeout
	serverOptions.v.dataCenter = serverDataCenter
	serverOptions.v.rack = serverRack
	mqBrokerOptions.ip = serverIp
	mqBrokerOptions.masters = filerOptions.masters.GetInstancesAsMap()
	mqBrokerOptions.filerGroup = filerOptions.filerGroup
	mqAgentServerOptions.ip = serverIp
	mqAgentServerOptions.brokers = pb.ServerAddresses(*mqAgentServerOptions.brokersString).ToAddresses()

	// serverOptions.v.pulseSeconds = pulseSeconds
	// masterOptions.pulseSeconds = pulseSeconds

	masterOptions.whiteList = serverWhiteListOption

	filerOptions.dataCenter = serverDataCenter
	filerOptions.rack = serverRack
	mqBrokerOptions.dataCenter = serverDataCenter
	mqBrokerOptions.rack = serverRack
	s3Options.dataCenter = serverDataCenter
	sftpOptions.dataCenter = serverDataCenter
	filerOptions.disableHttp = serverDisableHttp
	masterOptions.disableHttp = serverDisableHttp

	filerAddress := string(pb.NewServerAddress(*serverIp, *filerOptions.port, *filerOptions.portGrpc))
	s3Options.filer = &filerAddress
	sftpOptions.filer = &filerAddress
	iamOptions.filer = &filerAddress
	webdavOptions.filer = &filerAddress
	mqBrokerOptions.filerGroup = filerOptions.filerGroup

	go stats_collect.StartMetricsServer(*serverMetricsHttpIp, *serverMetricsHttpPort)

	*volumeDataFolders = util.ResolveCommaSeparatedPaths(*volumeDataFolders)
	folders := strings.Split(*volumeDataFolders, ",")

	if *masterOptions.volumeSizeLimitMB > util.VolumeSizeLimitGB*1000 {
		glog.Fatalf("masterVolumeSizeLimitMB should be less than 30000")
	}

	if *masterOptions.metaFolder == "" {
		*masterOptions.metaFolder = folders[0]
	}
	if err := util.TestFolderWritable(*masterOptions.metaFolder); err != nil {
		glog.Fatalf("Check Meta Folder (-mdir=\"%s\") Writable: %s", *masterOptions.metaFolder, err)
	}
	filerOptions.defaultLevelDbDirectory = masterOptions.metaFolder

	// Register Unix socket paths for gRPC services running in this process
	// so local inter-service communication uses Unix sockets instead of TCP.
	// Resolve gRPC ports early (same calculation each service does internally).
	for _, svc := range []struct {
		starting *bool
		portGrpc *int
		port     *int
		name     string
	}{
		{isStartingMasterServer, masterOptions.portGrpc, masterOptions.port, "master"},
		{isStartingVolumeServer, serverOptions.v.portGrpc, serverOptions.v.port, "volume"},
		{isStartingFiler, filerOptions.portGrpc, filerOptions.port, "filer"},
		{isStartingS3, s3Options.portGrpc, s3Options.port, "s3"},
	} {
		if *svc.starting {
			if *svc.portGrpc == 0 {
				*svc.portGrpc = 10000 + *svc.port
			}
			pb.RegisterLocalGrpcSocket(*serverIp, *svc.portGrpc, fmt.Sprintf("/tmp/seaweedfs-%s-grpc-%d.sock", svc.name, *svc.portGrpc))
		}
	}

	serverWhiteList := util.StringSplit(*serverWhiteListOption, ",")

	if *isStartingFiler {
		if *filerOptions.diskType == "" && *serverOptions.v.diskType != "" {
			filerOptions.diskType = serverOptions.v.diskType
		}
		go func() {
			time.Sleep(1 * time.Second)
			filerOptions.startFiler()
		}()
	}

	if *isStartingS3 {
		// Handle IAM config: -s3.iam.config takes precedence over -iam.config
		if *s3Options.iamConfig == "" {
			*s3Options.iamConfig = *serverIamConfig
		} else if *serverIamConfig != "" && *s3Options.iamConfig != *serverIamConfig {
			glog.V(0).Infof("both -s3.iam.config(%s) and -iam.config(%s) provided; using -s3.iam.config", *s3Options.iamConfig, *serverIamConfig)
		}
		// Share the S3 static identity config file with the filer
		filerOptions.s3ConfigFile = s3Options.config
		go func() {
			time.Sleep(2 * time.Second)
			s3Options.localFilerSocket = filerOptions.localSocket
			s3Options.startS3Server()
		}()
	}

	if *isStartingSftp {
		go func() {
			time.Sleep(2 * time.Second)
			sftpOptions.localSocket = filerOptions.localSocket
			sftpOptions.startSftpServer()
		}()
	}

	if *isStartingIam {
		go func() {
			time.Sleep(2 * time.Second)
			iamOptions.startIamServer()
		}()
	}

	if *isStartingWebDav {
		go func() {
			time.Sleep(2 * time.Second)
			webdavOptions.startWebDav()

		}()
	}

	if *isStartingMqBroker {
		go func() {
			time.Sleep(2 * time.Second)
			mqBrokerOptions.startQueueServer()
		}()
	}

	if *isStartingMqAgent {
		go func() {
			time.Sleep(2 * time.Second)
			mqAgentServerOptions.startQueueAgent()
		}()
	}

	// start volume server
	if *isStartingVolumeServer {
		minFreeSpaces := util.MustParseMinFreeSpace(*volumeMinFreeSpace, *volumeMinFreeSpacePercent)
		go serverOptions.v.startVolumeServer(*volumeDataFolders, *volumeMaxDataVolumeCounts, *serverWhiteListOption, minFreeSpaces)
	}

	if *isStartingMasterServer {
		go startMaster(masterOptions, serverWhiteList)
	}

	select {}
}

func newHttpServer(h http.Handler, tlsConfig *tls.Config) *http.Server {
	s := &http.Server{
		Handler: h,
	}
	if tlsConfig != nil {
		s.TLSConfig = tlsConfig.Clone()
	}
	return s
}
