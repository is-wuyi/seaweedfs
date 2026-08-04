package command

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/util/version"

	"time"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iamapi"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"

	// Import credential stores to register them
	_ "github.com/seaweedfs/seaweedfs/weed/credential/filer_etc"
	_ "github.com/seaweedfs/seaweedfs/weed/credential/memory"
	_ "github.com/seaweedfs/seaweedfs/weed/credential/postgres"
)

var (
	iamStandaloneOptions IamOptions
)

type IamOptions struct {
	filer   *string
	masters *string
	ip      *string
	port    *int
}

func init() {
	cmdIam.Run = runIam // break init cycle
	iamStandaloneOptions.filer = cmdIam.Flag.String("filer", "localhost:8888", "comma-separated filer server addresses for high availability")
	iamStandaloneOptions.masters = cmdIam.Flag.String("master", "localhost:9333", "comma-separated master servers")
	iamStandaloneOptions.ip = cmdIam.Flag.String("ip", util.DetectedHostAddress(), "iam server http listen ip address")
	iamStandaloneOptions.port = cmdIam.Flag.Int("port", 8111, "iam server http listen port")
}

var cmdIam = &Command{
	UsageLine: "iam [-port=8111] [-filer=<ip:port>[,<ip:port>]...] [-master=<ip:port>,<ip:port>]",
	Short:     "[已弃用] 启动一个独立的 IAM API 兼容服务器",
	Long: `[已弃用] 启动一个独立的 IAM API 兼容服务器。

	弃用通知:
	独立的 'weed iam' 命令已弃用,将在未来版本中移除。

	IAM API 现在默认内嵌在 S3 服务器中。直接使用 'weed s3' 即可,
	它会在同一个端口上同时提供 S3 和 IAM API(默认通过 -iam=true 启用)。

	这样只需运行单个服务器而非两个独立服务器,简化了部署,
	遵循了 MinIO 和 Ceph RGW 所采用的模式。

	使用内嵌的 IAM API:
	  weed s3 -port=8333          # IAM API 在同一端口上可用

	禁用内嵌的 IAM API(如果你偏好旧行为):
	  weed s3 -iam=false          # 运行不带 IAM 的 S3
	  weed iam -port=8111         # 单独运行 IAM(已弃用)

可指定多个 filer 地址以实现高可用,以逗号分隔。`,
}

func runIam(cmd *Command, args []string) bool {
	glog.Warningf("================================================================================")
	glog.Warningf("DEPRECATION WARNING: 'weed iam' is deprecated and will be removed in a future release.")
	glog.Warningf("The IAM API is now embedded in 'weed s3' by default (use -iam=true, which is the default).")
	glog.Warningf("Please migrate to using 'weed s3' which provides both S3 and IAM APIs on the same port.")
	glog.Warningf("================================================================================")
	return iamStandaloneOptions.startIamServer()
}

func (iamopt *IamOptions) startIamServer() bool {
	filerAddresses := pb.ServerAddresses(*iamopt.filer).ToAddresses()

	util.LoadSecurityConfiguration()
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")
	for {
		err := pb.WithOneOfGrpcFilerClients(false, filerAddresses, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
			if err != nil {
				return fmt.Errorf("get filer configuration: %v", err)
			}
			glog.V(0).Infof("IAM read filer configuration: %s", resp)
			return nil
		})
		if err != nil {
			glog.V(0).Infof("wait to connect to filers %v", filerAddresses)
			time.Sleep(time.Second)
		} else {
			glog.V(0).Infof("connected to filers %v", filerAddresses)
			break
		}
	}

	masters := pb.ServerAddresses(*iamopt.masters).ToAddressMap()
	router := mux.NewRouter().SkipClean(true)
	iamApiServer, iamApiServer_err := iamapi.NewIamApiServer(router, &iamapi.IamServerOption{
		Masters:        masters,
		Filers:         filerAddresses,
		Port:           *iamopt.port,
		GrpcDialOption: grpcDialOption,
	})
	glog.V(0).Info("NewIamApiServer created")
	if iamApiServer_err != nil {
		glog.Fatalf("IAM API Server startup error: %v", iamApiServer_err)
	}

	// Register shutdown handler to prevent goroutine leak
	grace.OnInterrupt(func() {
		iamApiServer.Shutdown()
	})

	listenAddress := fmt.Sprintf(":%d", *iamopt.port)
	iamApiListener, iamApiLocalListener, err := util.NewIpAndLocalListeners(*iamopt.ip, *iamopt.port, time.Duration(10)*time.Second)
	if err != nil {
		glog.Fatalf("IAM API Server listener on %s error: %v", listenAddress, err)
	}

	glog.V(0).Infof("Start Seaweed IAM API Server %s at http port %d", version.Version(), *iamopt.port)
	if iamApiLocalListener != nil {
		go func() {
			if err = newHttpServer(router, nil).Serve(iamApiLocalListener); err != nil {
				glog.Errorf("IAM API Server Fail to serve: %v", err)
			}
		}()
	}
	if err = newHttpServer(router, nil).Serve(iamApiListener); err != nil {
		glog.Fatalf("IAM API Server Fail to serve: %v", err)
	}

	return true
}
