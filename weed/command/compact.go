package command

import (
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	cmdCompact.Run = runCompact // break init cycle
}

var cmdCompact = &Command{
	UsageLine: "compact -dir=/tmp -volumeId=234",
	Short:     "对 volume 文件运行 weed 工具 compact",
	Long: `强制执行 compact,从卷文件中移除已删除的文件。
  compact 后的 .dat 文件存储为 .cpd 文件。
  compact 后的 .idx 文件存储为 .cpx 文件。

  支持两种 compact 方式:
    * data:  基于 .dat 文件进行 compact,在 .idx 文件损坏时仍可工作。
    * index: 基于 .idx 文件进行 compact,在已发生删除但未写入 .dat 文件时仍可工作。

  `,
}

var (
	compactVolumePath        = cmdCompact.Flag.String("dir", ".", "data directory to store files")
	compactVolumeCollection  = cmdCompact.Flag.String("collection", "", "volume collection name")
	compactVolumeId          = cmdCompact.Flag.Int("volumeId", -1, "a volume id. The volume should already exist in the dir.")
	compactMethod            = cmdCompact.Flag.String("method", "data", "option to choose which compact method (data/index)")
	compactVolumePreallocate = cmdCompact.Flag.Int64("preallocateMB", 0, "preallocate volume disk space")
)

func runCompact(cmd *Command, args []string) bool {

	if *compactVolumeId == -1 {
		return false
	}

	*compactVolumePath = util.ResolvePath(*compactVolumePath)
	preallocateBytes := *compactVolumePreallocate * (1 << 20)

	vid := needle.VolumeId(*compactVolumeId)
	v, err := storage.NewVolume(*compactVolumePath, *compactVolumePath, *compactVolumeCollection, vid, storage.NeedleMapInMemory, nil, nil, preallocateBytes, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		glog.Fatalf("Load Volume [ERROR] %s\n", err)
	}

	opts := &storage.CompactOptions{
		PreallocateBytes:  preallocateBytes,
		MaxBytesPerSecond: 0, // unlimited
	}
	switch strings.ToLower(*compactMethod) {
	case "data":
		if err = v.CompactByVolumeData(opts); err != nil {
			glog.Fatalf("Compact Volume [ERROR] %s\n", err)
		}
	case "index":
		if err = v.CompactByIndex(opts); err != nil {
			glog.Fatalf("Compact Volume [ERROR] %s\n", err)
		}
	default:
		glog.Fatalf("unsupported compaction method %q", *compactMethod)
	}

	return true
}
