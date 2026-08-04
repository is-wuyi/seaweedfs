package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"

	"github.com/seaweedfs/seaweedfs/weed/pb"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"

	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

func init() {
	Commands = append(Commands, &commandVolumeTierUpload{})
}

type commandVolumeTierUpload struct {
}

func (c *commandVolumeTierUpload) Name() string {
	return "volume.tier.upload"
}

func (c *commandVolumeTierUpload) Help() string {
	return `将某个卷的 dat 文件上传到远程分层

	volume.tier.upload [-collection=""] [-fullPercent=95] [-quietFor=1h]
	volume.tier.upload [-collection=""] -volumeId=<volume_id> -dest=<storage_backend> [-keepLocalDatFile]

	例如:
	volume.tier.upload -volumeId=7 -dest=s3
	volume.tier.upload -volumeId=7 -dest=s3.default

	<storage_backend> 在 master.toml 中定义。
	例如,[storage.backend.s3.default] 中的 "s3.default"

	此命令会将某个卷的 dat 文件迁移到远程分层。

	SeaweedFS 提供了对大量文件可扩展且快速的本地访问,
	而云存储速度较慢但成本更低。如何将两者结合?

	通常数据遵循 80/20 法则:只有 20% 的数据会被频繁访问。
	我们可以将旧卷卸载到云端。

	这样,SeaweedFS 既能保持快速和可扩展,又能拥有无限的存储空间。
	只需增加更多本地 SeaweedFS volume 服务器即可提升吞吐量。

	索引文件仍然保留在本地,对远程文件同样采用 O(1) 的磁盘读取。

	每个副本保留各自的本地索引,指向同一个远程对象,
	因此分层后卷在读取时仍能保持其副本数。

`
}

func (c *commandVolumeTierUpload) HasTag(CommandTag) bool {
	return false
}

func (c *commandVolumeTierUpload) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	tierCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeId := tierCommand.Int("volumeId", 0, "the volume id")
	collection := tierCommand.String("collection", "", "the collection name")
	fullPercentage := tierCommand.Float64("fullPercent", 95, "the volume reaches the percentage of max volume size")
	quietPeriod := tierCommand.Duration("quietFor", 24*time.Hour, "select volumes without no writes for this period")
	dest := tierCommand.String("dest", "", "the target tier name")
	keepLocalDatFile := tierCommand.Bool("keepLocalDatFile", false, "whether keep local dat file")
	disk := tierCommand.String("disk", "", "[hdd|ssd|<tag>] hard drive or solid state drive or any tag")
	if err = tierCommand.Parse(args); err != nil {
		return nil
	}

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	vid := needle.VolumeId(*volumeId)

	// volumeId is provided
	if vid != 0 {
		return doVolumeTierUpload(commandEnv, writer, *collection, vid, *dest, *keepLocalDatFile)
	}

	var diskType *types.DiskType
	if disk != nil {
		_diskType := types.ToDiskType(*disk)
		diskType = &_diskType
	}

	// apply to all volumes in the collection
	// reusing collectVolumeIdsForEcEncode for now
	volumeIds, _, err := collectVolumeIdsForEcEncode(commandEnv, *collection, diskType, *fullPercentage, *quietPeriod, false)
	if err != nil {
		return err
	}
	fmt.Printf("tier upload volumes: %v\n", volumeIds)
	for _, vid := range volumeIds {
		if err = doVolumeTierUpload(commandEnv, writer, *collection, vid, *dest, *keepLocalDatFile); err != nil {
			return err
		}
	}

	return nil
}

func doVolumeTierUpload(commandEnv *CommandEnv, writer io.Writer, collection string, vid needle.VolumeId, dest string, keepLocalDatFile bool) (err error) {
	// find volume location
	topoInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return fmt.Errorf("collect topology info: %v", err)
	}

	existingLocations := collectVolumeTierUploadLocations(topoInfo, vid, collection, writer)

	if len(existingLocations) == 0 {
		if collection == "" {
			return fmt.Errorf("volume %d not found", vid)
		}
		return fmt.Errorf("volume %d not found in collection %s", vid, collection)
	}

	err = markVolumeReplicasWritable(context.Background(), commandEnv.option.GrpcDialOption, vid, existingLocations, false, false)
	if err != nil {
		return fmt.Errorf("mark volume %d as readonly on %s: %v", vid, existingLocations[0].Url, err)
	}

	// copy the .dat file to remote tier
	err = uploadDatToRemoteTier(commandEnv.option.GrpcDialOption, writer, vid, collection, existingLocations[0].ServerAddress(), dest, keepLocalDatFile)
	if err != nil {
		return fmt.Errorf("copy dat file for volume %d on %s to %s: %v", vid, existingLocations[0].Url, dest, err)
	}

	if keepLocalDatFile {
		return nil
	}
	// Re-copy the uploaded replica's .idx/.vif onto the other replicas instead
	// of deleting them: the remote object key lives only in the .vif, so losing
	// the single server holding it would orphan the volume.
	for i, location := range existingLocations {
		if i == 0 {
			continue
		}
		fmt.Fprintf(writer, "replicate remote volume %d metadata from %s to %s\n", vid, existingLocations[0].Url, location.Url)
		err = replicateVolumeToServer(context.Background(), commandEnv.option.GrpcDialOption, writer, vid, existingLocations[0].ServerAddress(), location.ServerAddress(), "")
		if err != nil {
			return fmt.Errorf("replicate volume %d from %s to %s: %v", vid, existingLocations[0].Url, location.Url, err)
		}
	}

	return nil
}

// collectVolumeTierUploadLocations lists the replica locations of a volume,
// putting an already-tiered replica first so a rerun after a partial failure
// reuses its remote object instead of uploading a second copy.
func collectVolumeTierUploadLocations(topoInfo *master_pb.TopologyInfo, vid needle.VolumeId, collection string, writer io.Writer) []wdclient.Location {
	var tiered, local []wdclient.Location
	eachDataNode(topoInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		for _, disk := range dn.DiskInfos {
			for _, vi := range disk.VolumeInfos {
				if needle.VolumeId(vi.Id) == vid && (collection == "" || vi.Collection == collection) {
					fmt.Fprintf(writer, "find volume %d from Url:%s, GrpcPort:%d, DC:%s\n", vid, dn.Id, dn.GrpcPort, string(dc))
					loc := wdclient.Location{
						Url:        dn.Id,
						PublicUrl:  dn.Id,
						GrpcPort:   int(dn.GrpcPort),
						DataCenter: string(dc),
					}
					if vi.RemoteStorageKey != "" {
						tiered = append(tiered, loc)
					} else {
						local = append(local, loc)
					}
				}
			}
		}
	})
	return append(tiered, local...)
}

func uploadDatToRemoteTier(grpcDialOption grpc.DialOption, writer io.Writer, volumeId needle.VolumeId, collection string, sourceVolumeServer pb.ServerAddress, dest string, keepLocalDatFile bool) error {

	err := operation.WithVolumeServerClient(true, sourceVolumeServer, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		stream, copyErr := volumeServerClient.VolumeTierMoveDatToRemote(context.Background(), &volume_server_pb.VolumeTierMoveDatToRemoteRequest{
			VolumeId:               uint32(volumeId),
			Collection:             collection,
			DestinationBackendName: dest,
			KeepLocalDatFile:       keepLocalDatFile,
		})

		if stream == nil {
			if copyErr == nil {
				// when the volume is already uploaded, VolumeTierMoveDatToRemote will return nil stream and nil error
				// so we should directly return in this caseAdd commentMore actions
				fmt.Fprintf(writer, "volume %v already uploaded", volumeId)
				return nil
			} else {
				return copyErr
			}
		}
		var lastProcessed int64
		for {
			resp, recvErr := stream.Recv()
			if recvErr != nil {
				if recvErr == io.EOF {
					break
				} else {
					return recvErr
				}
			}

			processingSpeed := float64(resp.Processed-lastProcessed) / 1024.0 / 1024.0

			fmt.Fprintf(writer, "copied %.2f%%, %d bytes, %.2fMB/s\n", resp.ProcessedPercentage, resp.Processed, processingSpeed)

			lastProcessed = resp.Processed
		}

		return copyErr
	})

	return err

}
