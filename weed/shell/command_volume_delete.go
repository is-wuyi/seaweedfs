package shell

import (
	"context"
	"flag"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func init() {
	Commands = append(Commands, &commandVolumeDelete{})
}

type commandVolumeDelete struct {
}

func (c *commandVolumeDelete) Name() string {
	return "volume.delete"
}

func (c *commandVolumeDelete) Help() string {
	return `从一个 volume 服务器删除一个存活的卷

	volume.delete -node <volume server host:port> -volumeId <volume id>

	此命令从一个 volume 服务器删除一个卷。

`
}

func (c *commandVolumeDelete) HasTag(CommandTag) bool {
	return false
}

func (c *commandVolumeDelete) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	volDeleteCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeIdInt := volDeleteCommand.Int("volumeId", 0, "the volume id")
	nodeStr := volDeleteCommand.String("node", "", "the volume server <host>:<port>")
	if err = volDeleteCommand.Parse(args); err != nil {
		return nil
	}

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	sourceVolumeServer := pb.ServerAddress(*nodeStr)

	volumeId := needle.VolumeId(*volumeIdInt)

	return deleteVolume(context.Background(), commandEnv.option.GrpcDialOption, volumeId, sourceVolumeServer, false, false)

}
