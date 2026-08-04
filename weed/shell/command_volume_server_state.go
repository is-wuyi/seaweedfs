package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"google.golang.org/protobuf/proto"
)

func init() {
	Commands = append(Commands, &commandVolumeServerState{})
}

type commandVolumeServerState struct {
	env    *CommandEnv
	writer io.Writer
}

func (c *commandVolumeServerState) Name() string {
	return "volumeServer.state"
}

func (c *commandVolumeServerState) Help() string {
	return `查询/更新 volume 服务器的状态设置

	volumeServer.state [-nodes <host:port>] [..状态标志...]

	此命令显示所提供节点列表的 volume 服务器状态标志;
	如果为空,则查询拓扑中的所有节点。
	例如:

	  volumeServer.state --nodes 192.168.10.111:9000,192.168.10.112:9000

	此外,如果提供了任何标志,这些标志会应用到所选节点。
	命令会显示每个节点在状态更新 *之后* 的结果状态。例如...

	  volumeServer.state --nodes 192.168.10.111:9000 --maintenanceOn

	...会将指定的 volume 服务器设置为维护模式。

`
}

func (c *commandVolumeServerState) HasTag(CommandTag) bool {
	return false
}

func (c *commandVolumeServerState) write(format string, a ...any) {
	format = strings.TrimRight(format, " ")
	if len(format) == 0 {
		format = "\n"
	}
	fmt.Fprintf(c.writer, format, a...)

	last := format[len(format)-1:]
	if last != "\n" && last != "\r" {
		fmt.Fprint(c.writer, "\n")
	}
}

func (c *commandVolumeServerState) bool2Str(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

func (c *commandVolumeServerState) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	vsStateCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	nodesStr := vsStateCommand.String("nodes", "", "comma-separated list of volume servers <host>:<port> (optional)")
	maintenanceOn := vsStateCommand.Bool("maintenanceOn", false, "enables maintenance mode on the server")
	maintenanceOff := vsStateCommand.Bool("maintenanceOff", false, "disables maintenance mode on the server")
	if err = vsStateCommand.Parse(args); err != nil {
		return err
	}

	if *maintenanceOn && *maintenanceOff {
		return fmt.Errorf("--maintenanceOn and --maintenanceOff are mutually exclusive")
	}

	volumeServerAddrs := []pb.ServerAddress{}
	if *nodesStr != "" {
		for _, addr := range strings.Split(*nodesStr, ",") {
			volumeServerAddrs = append(volumeServerAddrs, pb.ServerAddress(addr))
		}
	} else {
		dns, err := collectDataNodes(commandEnv, 0)
		if err != nil {
			return err
		}
		for _, dn := range dns {
			volumeServerAddrs = append(volumeServerAddrs, pb.ServerAddress(dn.Address))
		}
	}
	slices.Sort(volumeServerAddrs)
	if len(volumeServerAddrs) == 0 {
		return fmt.Errorf("no volume servers specified")
	}

	c.env = commandEnv
	c.writer = writer

	for _, addr := range volumeServerAddrs {
		state, err := c.getVolumeServerState(addr)
		if err != nil {
			return fmt.Errorf("failed to load state from %v: %v", addr, err)
		}

		stateOrig := &volume_server_pb.VolumeServerState{}
		proto.Merge(stateOrig, state)

		// apply updates, if any
		switch {
		case *maintenanceOn:
			state.Maintenance = true
		case *maintenanceOff:
			state.Maintenance = false
		}

		// update volume server state if settings changed
		if !proto.Equal(state, stateOrig) {
			state, err = c.setVolumeServerState(addr, state)
			if err != nil {
				return fmt.Errorf("failed to update state for %v: %v", addr, err)
			}
		}

		c.write("%v\t -> Maintenance mode: %s\n", addr.String(), c.bool2Str(state.GetMaintenance()))
	}

	return
}

func (c *commandVolumeServerState) getVolumeServerState(volumeServerAddress pb.ServerAddress) (*volume_server_pb.VolumeServerState, error) {
	var state *volume_server_pb.VolumeServerState

	err := operation.WithVolumeServerClient(false, volumeServerAddress, c.env.option.GrpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		res, err := volumeServerClient.GetState(context.Background(), &volume_server_pb.GetStateRequest{})
		if err != nil {
			return err
		}

		state = res.GetState()
		return nil
	})

	return state, err
}

func (c *commandVolumeServerState) setVolumeServerState(volumeServerAddress pb.ServerAddress, state *volume_server_pb.VolumeServerState) (*volume_server_pb.VolumeServerState, error) {
	var stateAfter *volume_server_pb.VolumeServerState

	err := operation.WithVolumeServerClient(false, volumeServerAddress, c.env.option.GrpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		res, err := volumeServerClient.SetState(context.Background(), &volume_server_pb.SetStateRequest{
			State: state,
		})
		if err != nil {
			return err
		}

		stateAfter = res.GetState()
		return nil
	})

	return stateAfter, err
}
