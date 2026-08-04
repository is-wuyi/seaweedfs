package shell

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func init() {
	Commands = append(Commands, &commandVolumeCopy{})
}

type commandVolumeCopy struct {
}

func (c *commandVolumeCopy) Name() string {
	return "volume.copy"
}

func (c *commandVolumeCopy) Help() string {
	return `将卷从一个 volume 服务器复制到另一个 volume 服务器

	volume.copy -source <source volume server host:port> -target <target volume server host:port> -volumeId <volume id>

	此命令将卷从一个 volume 服务器复制到另一个 volume 服务器。
	通常在复制之前应先卸载该卷。

`
}

func (c *commandVolumeCopy) HasTag(CommandTag) bool {
	return false
}

func (c *commandVolumeCopy) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	volCopyCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeIdInt := volCopyCommand.Int("volumeId", 0, "the volume id")
	sourceNodeStr := volCopyCommand.String("source", "", "the source volume server <host>:<port>")
	targetNodeStr := volCopyCommand.String("target", "", "the target volume server <host>:<port>")
	noLock := volCopyCommand.Bool("noLock", false, "do not lock the admin shell at one's own risk")
	if err = volCopyCommand.Parse(args); err != nil {
		return nil
	}

	if *noLock {
		commandEnv.noLock = true
	} else if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	sourceVolumeServer, targetVolumeServer := pb.ServerAddress(*sourceNodeStr), pb.ServerAddress(*targetNodeStr)

	volumeId := needle.VolumeId(*volumeIdInt)

	if sourceVolumeServer == targetVolumeServer {
		return fmt.Errorf("source and target volume servers are the same!")
	}

	_, err = copyVolume(context.Background(), commandEnv.option.GrpcDialOption, writer, volumeId, sourceVolumeServer, targetVolumeServer, "", 0, true)
	return
}
