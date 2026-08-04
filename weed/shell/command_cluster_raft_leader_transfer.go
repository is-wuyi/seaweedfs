package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func init() {
	Commands = append(Commands, &commandRaftLeaderTransfer{})
}

type commandRaftLeaderTransfer struct{}

func (c *commandRaftLeaderTransfer) Name() string {
	return "cluster.raft.transferLeader"
}

func (c *commandRaftLeaderTransfer) Help() string {
	return `将 raft leader 转移到另一台 master 服务器

	此命令启动一次从当前 leader 到另一台服务器的优雅 leader 转移。
	在对当前 leader 执行维护之前使用此命令,以减少 filer 及其他组件中的错误。

	示例:
		# 转移到任意合格的 follower(自动选择)
		cluster.raft.transferLeader

		# 转移到指定服务器
		cluster.raft.transferLeader -id <server_id> -address <server_grpc_address>

	注意:
		- 需要 hashicorp raft(master 上设置 -raftHashicorp=true)
		- 此命令必须发送给当前 leader
		- 目标服务器必须是 raft 集群中具有投票权的成员
		- 使用 'cluster.raft.ps' 列出可用服务器并确定 leader
`
}

func (c *commandRaftLeaderTransfer) HasTag(CommandTag) bool {
	return false
}

func (c *commandRaftLeaderTransfer) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	leaderTransferCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	targetId := leaderTransferCommand.String("id", "", "target server id (must be used with -address)")
	targetAddress := leaderTransferCommand.String("address", "", "target server grpc address (must be used with -id)")

	if err := leaderTransferCommand.Parse(args); err != nil {
		return err
	}

	// Validate: id and address must be specified together
	if *targetId != "" && *targetAddress == "" {
		return fmt.Errorf("-address is required when -id is specified")
	}
	if *targetAddress != "" && *targetId == "" {
		return fmt.Errorf("-id is required when -address is specified")
	}

	// First, show current cluster status
	fmt.Fprintf(writer, "Checking current raft cluster status...\n")

	var currentLeader string
	err := commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		resp, err := client.RaftListClusterServers(ctx, &master_pb.RaftListClusterServersRequest{})
		if err != nil {
			return fmt.Errorf("failed to list cluster servers: %v", err)
		}

		if len(resp.ClusterServers) == 0 {
			fmt.Fprintf(writer, "No raft cluster configured (single master mode)\n")
			return fmt.Errorf("leadership transfer not available in single master mode")
		}

		fmt.Fprintf(writer, "Raft cluster has %d servers:\n", len(resp.ClusterServers))
		for _, server := range resp.ClusterServers {
			suffix := ""
			if server.IsLeader {
				suffix = " <- current leader"
				currentLeader = server.Id
			}
			fmt.Fprintf(writer, "  %s %s [%s]%s\n", server.Id, server.Address, server.Suffrage, suffix)
		}
		return nil
	})
	if err != nil {
		return err
	}

	if currentLeader == "" {
		return fmt.Errorf("no leader found in cluster")
	}

	// Perform the transfer
	targetDesc := "any eligible follower"
	if *targetId != "" {
		targetDesc = fmt.Sprintf("server %s (%s)", *targetId, *targetAddress)
	}
	fmt.Fprintf(writer, "\nTransferring leadership from %s to %s...\n", currentLeader, targetDesc)

	err = commandEnv.MasterClient.WithClient(true, func(client master_pb.SeaweedClient) error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		resp, err := client.RaftLeadershipTransfer(ctx, &master_pb.RaftLeadershipTransferRequest{
			TargetId:      *targetId,
			TargetAddress: *targetAddress,
		})
		if err != nil {
			return fmt.Errorf("leadership transfer failed: %v", err)
		}

		if resp.PreviousLeader != resp.NewLeader {
			fmt.Fprintf(writer, "Leadership successfully transferred.\n")
			fmt.Fprintf(writer, "  Previous leader: %s\n", resp.PreviousLeader)
			fmt.Fprintf(writer, "  New leader: %s\n", resp.NewLeader)
		} else {
			fmt.Fprintf(writer, "Leadership transfer initiated, but the same leader was re-elected.\n")
			fmt.Fprintf(writer, "  Current leader: %s\n", resp.NewLeader)
		}
		return nil
	})

	if err != nil {
		fmt.Fprintf(writer, "\nLeadership transfer failed: %v\n", err)
		fmt.Fprintf(writer, "\nTroubleshooting:\n")
		fmt.Fprintf(writer, "  - Ensure you are connected to the current leader\n")
		fmt.Fprintf(writer, "  - Ensure target server is a voting member (use 'cluster.raft.ps')\n")
		fmt.Fprintf(writer, "  - Ensure target server is healthy and reachable\n")
		return err
	}

	return nil
}
