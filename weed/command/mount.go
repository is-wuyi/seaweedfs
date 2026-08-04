package command

import (
	"os"
	"time"
)

type MountOptions struct {
	filer                *string
	filerMountRootPath   *string
	dir                  *string
	dirAutoCreate        *bool
	collection           *string
	collectionQuota      *int
	logicalDiskUsage     *bool
	replication          *string
	diskType             *string
	ttlSec               *int
	chunkSizeLimitMB     *int
	concurrentWriters    *int
	concurrentReaders    *int
	cacheMetaTtlSec      *int
	cacheDirForRead      *string
	cacheDirForWrite     *string
	cacheSizeMBForRead   *int64
	writeBufferSizeMB    *int64
	dataCenter           *string
	allowOthers          *bool
	defaultPermissions   *bool
	umaskString          *string
	nonempty             *bool
	volumeServerAccess   *string
	uidMap               *string
	gidMap               *string
	readOnly             *bool
	includeSystemEntries *bool
	debug                *bool
	debugPort            *int
	debugFuse            *bool
	localSocket          *string
	disableXAttr         *bool
	extraOptions         []string
	fuseCommandPid       int

	// Periodic metadata flush to protect against orphan chunk cleanup
	metadataFlushSeconds *int

	// RDMA acceleration options
	rdmaEnabled       *bool
	rdmaSidecarAddr   *string
	rdmaFallback      *bool
	rdmaReadOnly      *bool
	rdmaMaxConcurrent *int
	rdmaTimeoutMs     *int

	// Peer chunk sharing options (design-weed-mount-peer-chunk-sharing.md).
	peerEnabled      *bool
	peerListen       *string
	peerAdvertise    *string
	peerDataCenter   *string
	peerRack         *string

	dirIdleEvictSec *int

	// Distributed locking for cross-mount write coordination and POSIX
	// advisory locks (flock/fcntl)
	distributedLock *bool

	// POSIX compliance options
	posixDirNlink *bool

	// FUSE performance options
	writebackCache          *bool
	asyncDio                *bool
	cacheSymlink            *bool
	fuseMaxBackground       *int
	fuseCongestionThreshold *int

	// macOS-specific FUSE options
	novncache *bool

	// if true, we assume autofs exists over current mount point. Autofs (the kernel one, used by systemd automount)
	// is expected to be mounted as a shim between auto-mounted fs and original mount point to provide auto mount.
	// with this option, we ignore autofs mounted on the same point.
	hasAutofs *bool
}

var (
	mountOptions       MountOptions
	mountCpuProfile    *string
	mountMemProfile    *string
	mountReadRetryTime *time.Duration
)

func init() {
	cmdMount.Run = runMount // break init cycle
	mountOptions.filer = cmdMount.Flag.String("filer", "localhost:8888", "逗号分隔的 weed filer 位置")
	mountOptions.filerMountRootPath = cmdMount.Flag.String("filer.path", "/", "从 filer 服务器挂载此远程路径")
	mountOptions.dir = cmdMount.Flag.String("dir", ".", "将 weed filer 挂载到此目录")
	mountOptions.dirAutoCreate = cmdMount.Flag.Bool("dirAutoCreate", false, "自动创建要挂载到的目录")
	mountOptions.collection = cmdMount.Flag.String("collection", "", "创建文件所用的集合")
	mountOptions.collectionQuota = cmdMount.Flag.Int("collectionQuotaMB", 0, "集合的配额")
	mountOptions.logicalDiskUsage = cmdMount.Flag.Bool("df.logical", false, "向 df 和配额上报数据大小,而非包含副本和 EC 校验所占用的空间")
	mountOptions.replication = cmdMount.Flag.String("replication", "", "创建文件所用的副本策略(例如 000、001)。为空则交由 filer 决定。")
	mountOptions.diskType = cmdMount.Flag.String("disk", "", "[hdd|ssd|<tag>] 机械硬盘、固态硬盘或任意标签")
	mountOptions.ttlSec = cmdMount.Flag.Int("ttl", 0, "文件 TTL,单位为秒")
	mountOptions.chunkSizeLimitMB = cmdMount.Flag.Int("chunkSizeLimitMB", 2, "本地写缓冲区大小,同时用于拆分大文件")
	mountOptions.concurrentWriters = cmdMount.Flag.Int("concurrentWriters", 128, "限制并发 goroutine 写入数")
	mountOptions.concurrentReaders = cmdMount.Flag.Int("concurrentReaders", 128, "限制读操作的并发 chunk 拉取数")
	mountOptions.cacheDirForRead = cmdMount.Flag.String("cacheDir", os.TempDir(), "文件 chunk 和元数据的本地缓存目录")
	mountOptions.cacheSizeMBForRead = cmdMount.Flag.Int64("cacheCapacityMB", 128, "file chunk read cache capacity in MB")
	mountOptions.cacheDirForWrite = cmdMount.Flag.String("cacheDirWrite", "", "缓冲写入,主要用于大文件")
	mountOptions.writeBufferSizeMB = cmdMount.Flag.Int64("writeBufferSizeMB", 0, "global cap on the per-mount write buffer (memory + swap) in MB, 0 means unlimited. Bounds /tmp growth when volume uploads stall")
	mountOptions.cacheMetaTtlSec = cmdMount.Flag.Int("cacheMetaTtlSec", 60, "元数据缓存有效期,单位为秒")
	mountOptions.dataCenter = cmdMount.Flag.String("dataCenter", "", "优先写入该数据中心")
	mountOptions.allowOthers = cmdMount.Flag.Bool("allowOthers", true, "允许其他用户访问该文件系统")
	mountOptions.defaultPermissions = cmdMount.Flag.Bool("defaultPermissions", true, "由操作系统强制执行权限")
	mountOptions.umaskString = cmdMount.Flag.String("umask", "022", "八进制 umask,例如 022、0111")
	mountOptions.nonempty = cmdMount.Flag.Bool("nonempty", false, "允许在非空目录上挂载")
	mountOptions.volumeServerAccess = cmdMount.Flag.String("volumeServerAccess", "direct", "通过 [direct|publicUrl|filerProxy] 访问 volume 服务器")
	mountOptions.uidMap = cmdMount.Flag.String("map.uid", "", "将本地 uid 映射到 filer 上的 uid,逗号分隔 <local_uid>:<filer_uid>")
	mountOptions.gidMap = cmdMount.Flag.String("map.gid", "", "将本地 gid 映射到 filer 上的 gid,逗号分隔 <local_gid>:<filer_gid>")
	mountOptions.readOnly = cmdMount.Flag.Bool("readOnly", false, "只读")
	mountOptions.includeSystemEntries = cmdMount.Flag.Bool("includeSystemEntries", false, "在目录列表中显示 filer 系统条目(例如 /topics、/etc)")
	mountOptions.debug = cmdMount.Flag.Bool("debug", false, "提供运行时性能分析数据,例如 http://localhost:<debug.port>/debug/pprof/goroutine?debug=2")
	mountOptions.debugPort = cmdMount.Flag.Int("debug.port", 6061, "用于调试的 http 端口")
	mountOptions.debugFuse = cmdMount.Flag.Bool("debug.fuse", false, "记录原始 FUSE 协议请求与响应")
	mountOptions.localSocket = cmdMount.Flag.String("localSocket", "", "默认为 /tmp/seaweedfs-mount-<mount_dir_hash>.sock")
	mountOptions.disableXAttr = cmdMount.Flag.Bool("disableXAttr", false, "禁用 xattr")
	mountOptions.hasAutofs = cmdMount.Flag.Bool("autofs", false, "忽略挂载在同一挂载点的 autofs(在使用 systemd.automount 和 autofs 时有用)")
	mountOptions.fuseCommandPid = 0

	// Periodic metadata flush to protect against orphan chunk cleanup
	mountOptions.metadataFlushSeconds = cmdMount.Flag.Int("metadataFlushSeconds", 120, "定期向 filer 刷新文件元数据,单位为秒(0 表示禁用)。这可以防止长时间写入的 chunk 被 volume.fsck 清理")

	// RDMA acceleration flags
	mountOptions.rdmaEnabled = cmdMount.Flag.Bool("rdma.enabled", false, "为读取启用 RDMA 加速")
	mountOptions.rdmaSidecarAddr = cmdMount.Flag.String("rdma.sidecar", "", "RDMA sidecar 地址(例如 localhost:8081)")
	mountOptions.rdmaFallback = cmdMount.Flag.Bool("rdma.fallback", true, "RDMA 失败时回退到 HTTP")
	mountOptions.rdmaReadOnly = cmdMount.Flag.Bool("rdma.readOnly", false, "仅对读取使用 RDMA(写入使用 HTTP)")
	mountOptions.rdmaMaxConcurrent = cmdMount.Flag.Int("rdma.maxConcurrent", 64, "最大并发 RDMA 操作数")
	mountOptions.rdmaTimeoutMs = cmdMount.Flag.Int("rdma.timeoutMs", 5000, "RDMA 操作超时,单位为毫秒")

	// Peer chunk sharing flags.
	mountOptions.peerEnabled = cmdMount.Flag.Bool("peer.enable", false, "启用 peer chunk 共享 — 挂载点向其他挂载点提供其 chunk 缓存,并在可用时从 peer 获取而非从 volume 服务器获取")
	mountOptions.peerListen = cmdMount.Flag.String("peer.listen", ":18080", "peer gRPC 的绑定地址(目录 RPC + FetchChunk 流式传输)")
	mountOptions.peerAdvertise = cmdMount.Flag.String("peer.advertise", "", "其他挂载点用于访问本挂载点的外部可达 host:port(默认为自动检测的主机 + -peer.listen 端口)")
	mountOptions.peerDataCenter = cmdMount.Flag.String("peer.dataCenter", "", "向 peer 宣告的可选数据中心标签;与 -peer.rack 配合用于两级局部性排序")
	mountOptions.peerRack = cmdMount.Flag.String("peer.rack", "", "向 peer 宣告的可选机架标签")

	mountOptions.dirIdleEvictSec = cmdMount.Flag.Int("dirIdleEvictSec", 600, "驱逐空闲缓存目录的秒数(0 表示禁用)")

	mountCpuProfile = cmdMount.Flag.String("cpuprofile", "", "CPU 性能分析输出文件")
	mountMemProfile = cmdMount.Flag.String("memprofile", "", "内存性能分析输出文件")
	mountReadRetryTime = cmdMount.Flag.Duration("readRetryTime", 6*time.Second, "最大读取重试等待时间")

	// Distributed lock for cross-mount write coordination
	mountOptions.distributedLock = cmdMount.Flag.Bool("dlm", false, "在多个挂载点之间协调写入(同一时刻仅一个挂载点写入一个文件),并通过将 POSIX 建议锁(flock/fcntl)路由到属主 filer 来在挂载点间生效")

	// POSIX compliance options
	mountOptions.posixDirNlink = cmdMount.Flag.Bool("posix.dirNLink", false, "上报符合 POSIX 的目录 nlink(2 + 子目录数);每次 stat 需多一次目录列表")

	// FUSE performance options
	mountOptions.writebackCache = cmdMount.Flag.Bool("writebackCache", false, "启用 FUSE 写回缓存以提升写入性能(崩溃时有数据丢失风险)")
	mountOptions.asyncDio = cmdMount.Flag.Bool("asyncDio", false, "启用异步直接 I/O 以获得更好的并发性")
	mountOptions.cacheSymlink = cmdMount.Flag.Bool("cacheSymlink", false, "启用符号链接缓存以减少元数据查找")
	mountOptions.fuseMaxBackground = cmdMount.Flag.Int("fuse.maxBackground", 128, "FUSE max_background:内核将排队的最大在途异步请求数。重度上传负载可受益于更大的值(例如 2048)。等价于写入 /sys/fs/fuse/connections/<id>/max_background。若 -fuse.congestionThreshold 为 0,内核将其推导为该值的 3/4。")
	mountOptions.fuseCongestionThreshold = cmdMount.Flag.Int("fuse.congestionThreshold", 0, "FUSE congestion_threshold:当在途异步请求数达到该值时,内核将 FUSE bdi 标记为拥塞并限制新提交。0 表示使用默认值(-fuse.maxBackground 的 3/4)。等价于写入 /sys/fs/fuse/connections/<id>/congestion_threshold。当设置值高于 -fuse.maxBackground 时,内核会静默地将其钳制为 -fuse.maxBackground。")

	// macOS-specific FUSE options
	mountOptions.novncache = cmdMount.Flag.Bool("sys.novncache", false, "(仅 macOS)禁用 vnode 名称缓存以避免数据陈旧")
}

var cmdMount = &Command{
	UsageLine: "mount -filer=localhost:8888 -dir=/some/dir",
	Short:     "将 weed filer 挂载为用户态文件系统(FUSE)的本地目录",
	Long: `将 weed filer 挂载到用户态。

  前置条件:
  1) 有正在运行的 SeaweedFS master 和 volume 服务器
  2) 有正在运行的 "weed filer"
  这两个要求可通过一条命令 "weed server -filer=true" 实现

  这使用了 github.com/seaweedfs/fuse,它支持在
  Linux 和 OS X 上编写 FUSE 文件系统。

  在 OS X 上,需要 OSXFUSE (https://osxfuse.github.io/)。

  RDMA 加速:
  要实现超快读取,可通过 RDMA sidecar 启用 RDMA 加速:
    weed mount -filer=localhost:8888 -dir=/mnt/seaweedfs \
      -rdma.enabled=true -rdma.sidecar=localhost:8081

  RDMA 选项:
    -rdma.enabled=false          为读取启用 RDMA 加速
    -rdma.sidecar=""             RDMA sidecar 地址(启用时必需)
    -rdma.fallback=true          RDMA 失败时回退到 HTTP
    -rdma.readOnly=false         仅对读取使用 RDMA(写入使用 HTTP)
    -rdma.maxConcurrent=64       最大并发 RDMA 操作数
    -rdma.timeoutMs=5000         RDMA 操作超时(毫秒)

  `,
}
