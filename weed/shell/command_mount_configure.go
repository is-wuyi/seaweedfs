package shell

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb/mount_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/resolver/passthrough"
)

func init() {
	Commands = append(Commands, &commandMountConfigure{})
}

type commandMountConfigure struct {
}

func (c *commandMountConfigure) Name() string {
	return "mount.configure"
}

func (c *commandMountConfigure) Help() string {
	return `在当前服务器上配置挂载

	mount.configure -dir=<mount_directory>

	此命令通过 unix socket 与本地挂载通信,因此只能在本地运行。
	"mount_directory" 值需要与 "weed mount -dir=<mount_directory>" 启动挂载时完全一致

`
}

func (c *commandMountConfigure) HasTag(CommandTag) bool {
	return false
}

func (c *commandMountConfigure) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	mountConfigureCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	mountDir := mountConfigureCommand.String("dir", "", "the mount directory same as how \"weed mount -dir=<mount_directory>\" was started")
	mountQuota := mountConfigureCommand.Int("quotaMB", 0, "the quota in MB")
	if err = mountConfigureCommand.Parse(args); err != nil {
		return nil
	}

	mountDirHash := util.HashToInt32([]byte(*mountDir))
	if mountDirHash < 0 {
		mountDirHash = -mountDirHash
	}
	localSocket := fmt.Sprintf("/tmp/seaweedfs-mount-%d.sock", mountDirHash)

	clientConn, err := grpc.NewClient("passthrough:///unix://"+localSocket, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return
	}
	defer clientConn.Close()

	client := mount_pb.NewSeaweedMountClient(clientConn)
	_, err = client.Configure(context.Background(), &mount_pb.ConfigureRequest{
		CollectionCapacity: int64(*mountQuota) * 1024 * 1024,
	})

	return
}
