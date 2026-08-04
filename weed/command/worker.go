package command

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"
)

var cmdWorker = &Command{
	UsageLine: "worker -admin=<admin_server> [-id=<worker_id>] [-jobType=all] [-workingDir=<path>] [-heartbeat=15s] [-reconnect=5s] [-maxDetect=1] [-maxExecute=4] [-metricsPort=<port>] [-metricsIp=<ip>] [-debug]",
	Short:     "启动一个 plugin.proto worker 进程",
	Long: `通过 gRPC 启动一个使用 weed/pb/plugin.proto 的外部 plugin worker。

此命令提供用于集群维护的 plugin 作业类型处理器,
包括描述符投递、心跳/负载上报、探测和执行。

行为:
  - 使用 -jobType 按类别或显式名称(逗号分隔)选择处理器
  - 类别:"all"(所有已注册处理器)、"default"(轻量作业)、
    "heavy"(资源密集型作业,如纠删码)
  - 仍支持显式作业类型名称和别名(例如 "vacuum"、"ec")
  - 类别和显式名称可混合使用(例如 "default,iceberg")
  - 使用 -workingDir 持久化 worker.id,以便在重启后保持稳定的 worker 身份
  - 使用 -metricsPort/-metricsIp 暴露 /health、/ready 和 /metrics

示例:
  weed worker -admin=localhost:23646
  weed worker -admin=localhost:23646 -jobType=all
  weed worker -admin=localhost:23646 -jobType=default
  weed worker -admin=localhost:23646 -jobType=heavy
  weed worker -admin=localhost:23646 -jobType=default,iceberg
  weed worker -admin=localhost:23646 -jobType=vacuum,volume_balance
  weed worker -admin=localhost:23646 -jobType=erasure_coding
  weed worker -admin=admin.example.com:23646 -id=plugin-vacuum-a -heartbeat=10s
  weed worker -admin=localhost:23646 -workingDir=/var/lib/seaweedfs-plugin
  weed worker -admin=localhost:23646 -metricsPort=9327 -metricsIp=0.0.0.0
`,
}

var (
	workerAdminServer = cmdWorker.Flag.String("admin", "localhost:23646", "admin server address")
	workerID          = cmdWorker.Flag.String("id", "", "worker ID (auto-generated when empty)")
	workerWorkingDir  = cmdWorker.Flag.String("workingDir", "", "working directory for persistent worker state")
	workerJobType     = cmdWorker.Flag.String("jobType", defaultPluginWorkerJobTypes, "job types or categories to serve: all, default, heavy, or explicit names/aliases such as ec, balance, iceberg (comma-separated)")
	workerHeartbeat   = cmdWorker.Flag.Duration("heartbeat", 15*time.Second, "heartbeat interval")
	workerReconnect   = cmdWorker.Flag.Duration("reconnect", 5*time.Second, "reconnect delay")
	workerMaxDetect   = cmdWorker.Flag.Int("maxDetect", 1, "max concurrent detection requests")
	workerMaxExecute  = cmdWorker.Flag.Int("maxExecute", 4, "max concurrent execute requests")
	workerAddress     = cmdWorker.Flag.String("address", "", "worker address advertised to admin")
	workerMetricsPort = cmdWorker.Flag.Int("metricsPort", 0, "Prometheus metrics listen port")
	workerMetricsIp   = cmdWorker.Flag.String("metricsIp", "0.0.0.0", "Prometheus metrics listen IP")
	workerDebug       = cmdWorker.Flag.Bool("debug", false, "serves runtime profiling data via pprof on the port specified by -debug.port")
	workerDebugPort   = cmdWorker.Flag.Int("debug.port", 6060, "http port for debugging")
)

func init() {
	cmdWorker.Run = runWorker
}

func runWorker(cmd *Command, args []string) bool {
	if *workerDebug {
		grace.StartDebugServer(*workerDebugPort)
	}

	*workerWorkingDir = util.ResolvePath(*workerWorkingDir)
	return runPluginWorkerWithOptions(pluginWorkerRunOptions{
		AdminServer: *workerAdminServer,
		WorkerID:    *workerID,
		WorkingDir:  *workerWorkingDir,
		JobTypes:    *workerJobType,
		Heartbeat:   *workerHeartbeat,
		Reconnect:   *workerReconnect,
		MaxDetect:   *workerMaxDetect,
		MaxExecute:  *workerMaxExecute,
		Address:     *workerAddress,
		MetricsPort: *workerMetricsPort,
		MetricsIP:   *workerMetricsIp,
	})
}
