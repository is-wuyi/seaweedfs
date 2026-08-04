package command

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	mqKafkaGatewayOptions mqKafkaGatewayOpts
)

type mqKafkaGatewayOpts struct {
	ip                *string
	ipBind            *string
	port              *int
	pprofPort         *int
	master            *string
	filerGroup        *string
	schemaRegistryURL *string
	defaultPartitions *int
}

func init() {
	cmdMqKafkaGateway.Run = runMqKafkaGateway
	mqKafkaGatewayOptions.ip = cmdMqKafkaGateway.Flag.String("ip", util.DetectedHostAddress(), "Kafka gateway advertised host address")
	mqKafkaGatewayOptions.ipBind = cmdMqKafkaGateway.Flag.String("ip.bind", "", "Kafka gateway bind address (default: same as -ip)")
	mqKafkaGatewayOptions.port = cmdMqKafkaGateway.Flag.Int("port", 9092, "Kafka gateway listen port")
	mqKafkaGatewayOptions.pprofPort = cmdMqKafkaGateway.Flag.Int("port.pprof", 0, "HTTP profiling port (0 to disable)")
	mqKafkaGatewayOptions.master = cmdMqKafkaGateway.Flag.String("master", "localhost:9333", "comma-separated SeaweedFS master servers")
	mqKafkaGatewayOptions.filerGroup = cmdMqKafkaGateway.Flag.String("filerGroup", "", "filer group name")
	mqKafkaGatewayOptions.schemaRegistryURL = cmdMqKafkaGateway.Flag.String("schema-registry-url", "", "Schema Registry URL (required for schema management)")
	mqKafkaGatewayOptions.defaultPartitions = cmdMqKafkaGateway.Flag.Int("default-partitions", 4, "Default number of partitions for auto-created topics")
}

var cmdMqKafkaGateway = &Command{
	UsageLine: "mq.kafka.gateway [-ip=<host>] [-ip.bind=<bind_addr>] [-port=9092] [-master=<master_servers>] [-filerGroup=<group>] [-default-partitions=4] -schema-registry-url=<url>",
	Short:     "为 SeaweedMQ 启动一个带 schema 管理的 Kafka 线协议网关",
	Long: `启动一个 Kafka 线协议网关,将 Kafka 客户端请求转换为 SeaweedMQ 请求。

连接到 SeaweedFS master 服务器以发现可用的 broker,并与 Schema Registry 集成
以实现具备 schema 感知的 topic 管理。

选项:
  -ip                  客户端应连接的对外主机地址(默认:自动检测)
  -ip.bind             网关监听的绑定地址(默认:与 -ip 相同)
                       使用 0.0.0.0 绑定到所有接口,同时通告特定 IP
  -port                监听端口(默认:9092)
  -default-partitions  自动创建的 topic 的默认分区数(默认:4)
  -schema-registry-url Schema Registry URL(schema 管理所必需)

示例:
  weed mq.kafka.gateway -port=9092 -master=localhost:9333 -schema-registry-url=http://localhost:8081
  weed mq.kafka.gateway -ip=gateway1 -port=9092 -master=master1:9333,master2:9333 -schema-registry-url=http://schema-registry:8081
  weed mq.kafka.gateway -ip=external.host.com -ip.bind=0.0.0.0 -master=localhost:9333 -schema-registry-url=http://schema-registry:8081

这是实验性功能,目前仅支持用于开发的最小子集。
`,
}

func runMqKafkaGateway(cmd *Command, args []string) bool {
	// Validate required options
	if *mqKafkaGatewayOptions.master == "" {
		glog.Fatalf("SeaweedFS master address is required (-master)")
		return false
	}

	// Schema Registry URL is required for schema management
	if *mqKafkaGatewayOptions.schemaRegistryURL == "" {
		glog.Fatalf("Schema Registry URL is required (-schema-registry-url)")
		return false
	}

	// Determine bind address - default to advertised IP if not specified
	bindIP := *mqKafkaGatewayOptions.ipBind
	if bindIP == "" {
		bindIP = *mqKafkaGatewayOptions.ip
	}

	// Construct listen address from bind IP and port
	listenAddr := fmt.Sprintf("%s:%d", bindIP, *mqKafkaGatewayOptions.port)

	// Set advertised host for Kafka protocol handler
	if err := os.Setenv("KAFKA_ADVERTISED_HOST", *mqKafkaGatewayOptions.ip); err != nil {
		glog.Warningf("Failed to set KAFKA_ADVERTISED_HOST environment variable: %v", err)
	}

	srv := gateway.NewServer(gateway.Options{
		Listen:            listenAddr,
		Masters:           *mqKafkaGatewayOptions.master,
		FilerGroup:        *mqKafkaGatewayOptions.filerGroup,
		SchemaRegistryURL: *mqKafkaGatewayOptions.schemaRegistryURL,
		DefaultPartitions: int32(*mqKafkaGatewayOptions.defaultPartitions),
	})

	glog.Warningf("EXPERIMENTAL FEATURE: MQ Kafka Gateway is experimental and should NOT be used in production environments. It currently supports only a minimal subset of Kafka protocol for development purposes.")

	// Show bind vs advertised addresses for clarity
	if bindIP != *mqKafkaGatewayOptions.ip {
		glog.V(0).Infof("Starting MQ Kafka Gateway: binding to %s, advertising %s:%d to clients",
			listenAddr, *mqKafkaGatewayOptions.ip, *mqKafkaGatewayOptions.port)
	} else {
		glog.V(0).Infof("Starting MQ Kafka Gateway on %s", listenAddr)
	}
	glog.V(0).Infof("Using SeaweedMQ brokers from masters: %s", *mqKafkaGatewayOptions.master)

	// Start HTTP profiling server if enabled
	if *mqKafkaGatewayOptions.pprofPort > 0 {
		go func() {
			pprofAddr := fmt.Sprintf(":%d", *mqKafkaGatewayOptions.pprofPort)
			glog.V(0).Infof("Kafka Gateway pprof server listening on %s", pprofAddr)
			glog.V(0).Infof("Access profiling at: http://localhost:%d/debug/pprof/", *mqKafkaGatewayOptions.pprofPort)
			if err := http.ListenAndServe(pprofAddr, nil); err != nil {
				glog.Errorf("pprof server error: %v", err)
			}
		}()
	}

	if err := srv.Start(); err != nil {
		glog.Fatalf("mq kafka gateway start: %v", err)
		return false
	}

	// Set up graceful shutdown
	defer func() {
		glog.V(0).Infof("Shutting down MQ Kafka Gateway...")
		if err := srv.Close(); err != nil {
			glog.Errorf("mq kafka gateway close: %v", err)
		}
	}()

	// Serve blocks until closed
	if err := srv.Wait(); err != nil {
		glog.Errorf("mq kafka gateway wait: %v", err)
		return false
	}
	return true
}
