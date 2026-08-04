package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

const (
	ecShardActionTimeout = 1 * time.Minute
)

type ecShard struct {
	ShardID     uint32
	Collection  string
	NodeAddress string
}

func (s *ecShard) String() string {
	if s.NodeAddress == "" {
		return fmt.Sprintf("%d", s.ShardID)
	}
	return fmt.Sprintf("%d@%s", s.ShardID, s.NodeAddress)
}

func ecShardsFromString(shards string) ([]*ecShard, error) {
	res := []*ecShard{}

	for _, s := range strings.Split(shards, ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			return nil, fmt.Errorf("empty shard ID in %q", shards)
		}

		// optional <shard ID>@<node address> to pick one copy
		idStr, addr, _ := strings.Cut(s, "@")

		id, err := strconv.Atoi(idStr)
		if err != nil || id < 0 || id >= erasure_coding.MaxShardCount {
			return nil, fmt.Errorf("invalid shard ID %q", s)
		}

		res = append(res, &ecShard{ShardID: uint32(id), NodeAddress: addr})
	}

	return res, nil
}

func init() {
	Commands = append(Commands, &commandEcShardUnmount{})
}

type commandEcShardUnmount struct {
	env           *CommandEnv
	writer        io.Writer
	topology      *master_pb.TopologyInfo
	volumeID      uint32
	delete        bool
	ignoreInvalid bool
	apply         bool
	shards        []*ecShard
}

func (c *commandEcShardUnmount) Name() string {
	return "ec.shard.unmount"
}

func (c *commandEcShardUnmount) Help() string {
	return `卸载 EC 卷分片,并可选地删除它们。

	ec.shard.unmount --volumeId=<volume id> --shardId=<逗号分隔的分片 ID 列表> [--delete]

	分片 ID 可以指定为单个数字 ID(例如 "2"),也可以带节点地址
	(<shard_id>@<address>,例如 "3@10.200.18.88:9007"),以便在分片重复
	和/或共置于同一节点时选取其中一份副本。实时拓扑也以相同形式打印,
	因此可以直接原样复制其条目。

	卸载仅会在其 volume 服务器上将分片下线;传入 --delete 还会删除分片文件。
	这对于在不进行完整 rebalance 的情况下清理副本过多的 EC 卷很有用。

	分片会逐个尽力处理:出错时命令会停止,并保留已处理分片的当前状态。

	此命令如果使用不当,会且将会不可撤销地删除数据。请谨慎使用。
`
}

func (c *commandEcShardUnmount) HasTag(CommandTag) bool {
	return false
}

func (c *commandEcShardUnmount) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	if handleHelpRequest(c, args, writer) {
		return nil
	}

	ecShardUnmountCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeID := ecShardUnmountCommand.Uint("volumeId", 0, "volume ID for the shards to process")
	shardIDsStr := ecShardUnmountCommand.String("shardId", "", "comma-separated EC shard IDs for the volume")
	delete := ecShardUnmountCommand.Bool("delete", false, "also delete the shard files after unmounting them")
	ignoreInvalid := ecShardUnmountCommand.Bool("ignoreInvalid", false, "ignore user-provided shards that match nothing live, and try to proceed anyway")
	apply := ecShardUnmountCommand.Bool("apply", false, "Execute actions on the cluster. By default only lists what actions would be executed.")
	applyAlias := ecShardUnmountCommand.Bool("force", false, "Execute actions on the cluster (alias for -apply).")

	if err = ecShardUnmountCommand.Parse(args); err != nil {
		return err
	}
	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	if *volumeID == 0 {
		return fmt.Errorf("no volume ID")
	}
	if *volumeID > math.MaxUint32 {
		return fmt.Errorf("invalid volume ID %d", *volumeID)
	}

	if *shardIDsStr == "" {
		return fmt.Errorf("missing shardId")
	}
	shards, err := ecShardsFromString(*shardIDsStr)
	if err != nil {
		return err
	}

	// collect topology information
	topology, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}

	c.env = commandEnv
	c.writer = writer
	c.topology = topology
	c.volumeID = uint32(*volumeID)
	c.delete = *delete
	c.ignoreInvalid = *ignoreInvalid
	c.apply = *apply || *applyAlias
	c.shards = shards

	return c.doShardsUnmount()
}

func (c *commandEcShardUnmount) write(format string, a ...any) {
	fmt.Fprintf(c.writer, format, a...)
}

func (c *commandEcShardUnmount) liveShardsForVolume() []*ecShard {
	shards := []*ecShard{}

	for _, dci := range c.topology.GetDataCenterInfos() {
		for _, ri := range dci.GetRackInfos() {
			for _, dni := range ri.GetDataNodeInfos() {
				nodeAddress := dni.GetAddress()
				for _, di := range dni.GetDiskInfos() {
					for _, eci := range di.GetEcShardInfos() {
						if eci.GetId() == c.volumeID {
							sinfo := erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(eci)
							for _, sid := range sinfo.Ids() {
								shards = append(shards, &ecShard{
									ShardID:     uint32(sid),
									Collection:  eci.GetCollection(),
									NodeAddress: nodeAddress,
								})
							}
						}
					}
				}
			}
		}
	}

	sort.SliceStable(shards, func(i, j int) bool { return shards[i].ShardID < shards[j].ShardID })
	return shards
}

func (c *commandEcShardUnmount) printShards(ss []*ecShard) {
	for _, s := range ss {
		c.write("\t%v\n", s)
	}
	c.write("\n")
}

func (c *commandEcShardUnmount) doShardsUnmount() error {
	liveShards := c.liveShardsForVolume()
	c.write("Live shard topology for volume ID %d (%d shards):\n", c.volumeID, len(liveShards))
	c.printShards(liveShards)

	// resolve target shards against the live topology
	targetShards := []*ecShard{}
	for _, ps := range c.shards {
		var result *ecShard
		for _, ts := range liveShards {
			if ts.ShardID == ps.ShardID {
				if ps.NodeAddress == "" || ps.NodeAddress == ts.NodeAddress {
					if result != nil {
						return fmt.Errorf("shard %v is ambiguous", ps)
					}
					result = ts
				}
			}
		}
		if result == nil {
			if !c.ignoreInvalid {
				return fmt.Errorf("shard %v is invalid", ps)
			}
			c.write("!!! ignoring invalid shard %v\n", ps)
		} else {
			targetShards = append(targetShards, result)
		}
	}
	if len(targetShards) == 0 {
		return fmt.Errorf("got no shards to process")
	}

	mode := "unmount"
	if c.delete {
		mode = "unmount + delete"
	}
	c.write("Will %s %d shard(s):\n", mode, len(targetShards))
	c.printShards(targetShards)

	if !c.apply {
		c.write("Not proceeding in dry-run mode\n")
		return nil
	}

	for _, s := range targetShards {
		if err := c.unmountShard(s); err != nil {
			return err
		}
	}

	c.write("\nAll done!\n")
	return nil
}

func (c *commandEcShardUnmount) unmountShard(s *ecShard) error {
	ctx, cancel := context.WithTimeout(context.Background(), ecShardActionTimeout)
	defer cancel()

	return operation.WithVolumeServerClient(false, pb.ServerAddress(s.NodeAddress), c.env.option.GrpcDialOption, func(vsc volume_server_pb.VolumeServerClient) error {
		c.write("Unmounting shard %v for volume ID %d...\n", s, c.volumeID)
		if _, err := vsc.VolumeEcShardsUnmount(ctx, &volume_server_pb.VolumeEcShardsUnmountRequest{
			VolumeId: c.volumeID,
			ShardIds: []uint32{s.ShardID},
		}); err != nil {
			return err
		}

		if c.delete {
			c.write("Deleting shard %v for volume ID %d...\n", s, c.volumeID)
			if _, err := vsc.VolumeEcShardsDelete(ctx, &volume_server_pb.VolumeEcShardsDeleteRequest{
				VolumeId:   c.volumeID,
				Collection: s.Collection,
				ShardIds:   []uint32{s.ShardID},
			}); err != nil {
				return err
			}
		}

		return nil
	})
}
