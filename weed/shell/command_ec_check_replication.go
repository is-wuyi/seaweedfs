package shell

import (
	"flag"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

func init() {
	Commands = append(Commands, &commandEcCheckReplication{})
}

type commandEcCheckReplication struct {
}

func (c *commandEcCheckReplication) Name() string {
	return "ec.check.replication"
}

func (c *commandEcCheckReplication) Help() string {
	return `检查 EC 卷中分片副本不足或副本过多的情况

	ec.check.replication [-volumeIds=<id>,<id>...] [-details]

	报告符合以下情况的 EC 卷:
	  - 副本不足:每个节点都至少缺失一个分片
	  - 副本过多:至少一个分片有多于一份副本,无论位于
	            不同节点,还是同一节点的多个磁盘上

	在 ec.balance 或 ec.encode 运行期间,副本过多属于正常且暂时的现象,
	因为分片会先被复制再删除冗余副本;在这些操作完成后再重新检查,
	确认其是否为问题。

	每个卷按其自身的 data+parity 比例进行检查,而非固定的分片数,
	因此非默认 erasure coding 比例的卷也能被正确报告。

	选项:
	  -volumeIds:逗号分隔的要检查的 EC volume ID(默认:所有 EC 卷)
	  -details:   为每个被标记的卷打印每个分片的节点分布
`
}

func (c *commandEcCheckReplication) HasTag(CommandTag) bool {
	return false
}

func (c *commandEcCheckReplication) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	checkReplicationCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeIDsStr := checkReplicationCommand.String("volumeIds", "", "comma-separated EC volume IDs to process (optional)")
	showDetails := checkReplicationCommand.Bool("details", false, "display result details, if available")

	if err = checkReplicationCommand.Parse(args); err != nil {
		return err
	}
	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	// Keep execution state in a per-run runner rather than on the long-lived
	// command singleton, so concurrent or repeated invocations don't share state.
	runner := &ecCheckReplicationRunner{
		writer:      writer,
		volumeIDMap: map[uint32]bool{},
	}

	if *volumeIDsStr != "" {
		for _, vids := range strings.Split(*volumeIDsStr, ",") {
			vids = strings.TrimSpace(vids)
			if vids == "" {
				continue
			}
			if vid, err := strconv.ParseUint(vids, 10, 32); err == nil {
				runner.volumeIDMap[uint32(vid)] = true
			} else {
				return fmt.Errorf("invalid volume ID %q", vids)
			}
		}
	}

	runner.dataNodes, err = collectDataNodes(commandEnv, 0)
	if err != nil {
		return err
	}

	return runner.checkEcVolumes(*showDetails)
}

// ecCheckReplicationRunner holds the state for a single ec.check.replication invocation.
type ecCheckReplicationRunner struct {
	writer      io.Writer
	dataNodes   []*master_pb.DataNodeInfo
	volumeIDMap map[uint32]bool
}

func (r *ecCheckReplicationRunner) write(format string, a ...any) {
	fmt.Fprintf(r.writer, format, a...)
}

func (r *ecCheckReplicationRunner) isVolumeIDValid(vid uint32) bool {
	if len(r.volumeIDMap) == 0 {
		return true
	}
	return r.volumeIDMap[vid]
}

// ecVolumeShardReplication aggregates the observed shards for a single EC volume,
// together with the data+parity ratio the volume was encoded with. The ratio is
// taken per volume (via erasure_coding.EcShardsVolume*Shards) so custom EC
// ratios are checked against their own expected shard count.
type ecVolumeShardReplication struct {
	dataShards   int
	parityShards int
	// shardAddresses maps a shard id to the sorted node addresses hosting it.
	shardAddresses map[erasure_coding.ShardId][]string
}

func (h *ecVolumeShardReplication) totalShards() int {
	return h.dataShards + h.parityShards
}

// replicaCount is the total number of shard copies observed, counting each
// over-replicated shard once per hosting node.
func (h *ecVolumeShardReplication) replicaCount() int {
	n := 0
	for _, addrs := range h.shardAddresses {
		n += len(addrs)
	}
	return n
}

// unexpectedShardIds returns, in ascending order, any observed shard ids that
// fall outside the volume's expected data+parity range. These are anomalies
// (e.g. a stray shard) and are treated as over-replication.
func (h *ecVolumeShardReplication) unexpectedShardIds() []erasure_coding.ShardId {
	var ids []erasure_coding.ShardId
	for sid := range h.shardAddresses {
		if int(sid) >= h.totalShards() {
			ids = append(ids, sid)
		}
	}
	slices.Sort(ids)
	return ids
}

// TODO: check shard sizes?
func (r *ecCheckReplicationRunner) checkEcVolumes(showDetails bool) error {
	// collect EC shard placement, keyed by volume id
	volumes := map[uint32]*ecVolumeShardReplication{}
	for _, dni := range r.dataNodes {
		nodeAddress := dni.GetAddress()
		for _, di := range dni.GetDiskInfos() {
			for _, eci := range di.GetEcShardInfos() {
				vid := eci.GetId()
				if !r.isVolumeIDValid(vid) {
					continue
				}

				h, ok := volumes[vid]
				if !ok {
					// all shards of a volume share one ratio; take it from the
					// first shard message seen for the volume.
					h = &ecVolumeShardReplication{
						dataShards:     erasure_coding.EcShardsVolumeDataShards(eci),
						parityShards:   erasure_coding.EcShardsVolumeParityShards(eci),
						shardAddresses: map[erasure_coding.ShardId][]string{},
					}
					volumes[vid] = h
				}

				sinfo := erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(eci)
				for _, sid := range sinfo.Ids() {
					h.shardAddresses[sid] = append(h.shardAddresses[sid], nodeAddress)
				}
			}
		}
	}

	if len(volumes) == 0 {
		// Asking about specific volume IDs that turn out not to be EC volumes is
		// an error; an unfiltered run over a cluster with no EC volumes is a
		// legitimate, healthy state rather than a failure.
		if len(r.volumeIDMap) > 0 {
			return fmt.Errorf("no EC volumes found")
		}
		r.write("No EC volumes found.\n")
		return nil
	}

	// keep the shard address lists nicely sorted, for the sake of readability.
	for _, h := range volumes {
		for _, addrs := range h.shardAddresses {
			slices.Sort(addrs)
		}
	}

	// classify each volume against its own expected shard count
	underreplicatedVolumeIDs := []uint32{}
	overreplicatedVolumeIDs := []uint32{}
	for vid, h := range volumes {
		under := false
		over := false
		for sid := 0; sid < h.totalShards(); sid++ {
			switch len(h.shardAddresses[erasure_coding.ShardId(sid)]) {
			case 0:
				under = true
			case 1:
			default:
				over = true
			}
		}
		// shard ids beyond the expected data+parity range are unexpected extras,
		// i.e. redundant data the ratio doesn't call for.
		if len(h.unexpectedShardIds()) > 0 {
			over = true
		}

		// under- and over-replication are independent problems (missing shards risk
		// data loss, redundant shards waste space), so a volume exhibiting both is
		// reported in both lists.
		if under {
			underreplicatedVolumeIDs = append(underreplicatedVolumeIDs, vid)
		}
		if over {
			overreplicatedVolumeIDs = append(overreplicatedVolumeIDs, vid)
		}
	}
	slices.Sort(underreplicatedVolumeIDs)
	slices.Sort(overreplicatedVolumeIDs)

	// ...and display results
	if len(underreplicatedVolumeIDs) == 0 && len(overreplicatedVolumeIDs) == 0 {
		r.write("EC volumes are healthy.\n")
		return nil
	}

	if len(underreplicatedVolumeIDs) != 0 {
		r.write("Found %d/%d under-replicated EC volumes: %v\n", len(underreplicatedVolumeIDs), len(volumes), underreplicatedVolumeIDs)
	}
	if len(overreplicatedVolumeIDs) != 0 {
		r.write("Found %d/%d over-replicated EC volumes: %v\n", len(overreplicatedVolumeIDs), len(volumes), overreplicatedVolumeIDs)
	}

	if showDetails {
		if len(underreplicatedVolumeIDs) != 0 {
			r.write("\n")
			r.writeShardMaps("under-replicated", underreplicatedVolumeIDs, volumes)
		}
		if len(overreplicatedVolumeIDs) != 0 {
			r.write("\n")
			r.writeShardMaps("over-replicated", overreplicatedVolumeIDs, volumes)
		}
	}

	return nil
}

func (r *ecCheckReplicationRunner) writeShardMaps(kind string, volumeIDs []uint32, volumes map[uint32]*ecVolumeShardReplication) {
	for _, vid := range volumeIDs {
		h := volumes[vid]
		r.write("Shards map for %s EC volume %v (%d/%d shards):\n", kind, vid, h.replicaCount(), h.totalShards())
		for sid := 0; sid < h.totalShards(); sid++ {
			shardTypeDesc := ""
			if sid >= h.dataShards {
				shardTypeDesc = " (parity)"
			}
			if addrs, ok := h.shardAddresses[erasure_coding.ShardId(sid)]; ok {
				r.write("\t%02d%s => %v\n", sid, shardTypeDesc, addrs)
			} else {
				r.write("\t%02d%s is missing\n", sid, shardTypeDesc)
			}
		}
		for _, sid := range h.unexpectedShardIds() {
			r.write("\t%02d (unexpected) => %v\n", int(sid), h.shardAddresses[sid])
		}
	}
}
