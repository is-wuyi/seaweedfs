package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	Commands = append(Commands, &commandRemoteUncache{})
}

type commandRemoteUncache struct {
}

func (c *commandRemoteUncache) Name() string {
	return "remote.uncache"
}

func (c *commandRemoteUncache) Help() string {
	return `保留元数据但取消缓存已挂载目录或文件的文件内容

	此命令设计为定期运行，因此可以将其加入 cron 定时任务。
	如果文件未与远端副本同步，将被跳过以避免数据丢失。

	remote.uncache -dir=/xxx
	remote.uncache -dir=/xxx/some/sub/dir
	remote.uncache -dir=/xxx/some/sub/dir -include=*.pdf
	remote.uncache -dir=/xxx/some/sub/dir -exclude=*.txt
	remote.uncache -minSize=1024000    # 取消缓存大于 100K 的文件
	remote.uncache -minAge=3600        # 取消缓存超过 1 小时的文件（创建时间）
	remote.uncache -minCacheAge=3600   # 取消缓存超过 1 小时的文件（缓存时间）

`
}

func (c *commandRemoteUncache) HasTag(CommandTag) bool {
	return false
}

func (c *commandRemoteUncache) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	remoteUncacheCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)

	dir := remoteUncacheCommand.String("dir", "", "a directory in filer")
	fileFiler := newFileFilter(remoteUncacheCommand)

	if err = remoteUncacheCommand.Parse(args); err != nil {
		return nil
	}

	mappings, listErr := filer.ReadMountMappings(commandEnv.option.GrpcDialOption, commandEnv.option.FilerAddress)
	if listErr != nil {
		return listErr
	}
	if *dir != "" {
		var localMountedDir string
		for k := range mappings.Mappings {
			if strings.HasPrefix(*dir, k) {
				localMountedDir = k
			}
		}
		if localMountedDir == "" {
			jsonPrintln(writer, mappings)
			fmt.Fprintf(writer, "%s is not mounted\n", *dir)
			return nil
		}

		// pull content from remote
		if err = c.uncacheContentData(commandEnv, writer, util.FullPath(*dir), fileFiler); err != nil {
			return fmt.Errorf("uncache content data: %w", err)
		}
		return nil
	}

	for key, _ := range mappings.Mappings {
		if err := c.uncacheContentData(commandEnv, writer, util.FullPath(key), fileFiler); err != nil {
			return err
		}
	}

	return nil
}

func (c *commandRemoteUncache) uncacheContentData(commandEnv *CommandEnv, writer io.Writer, dirToCache util.FullPath, fileFilter *FileFilter) error {

	return recursivelyTraverseDirectory(commandEnv, dirToCache, func(dir util.FullPath, entry *filer_pb.Entry) bool {

		if !mayHaveCachedToLocal(entry) {
			return true // true means recursive traversal should continue
		}

		if !fileFilter.matches(entry) {
			return true
		}

		if entry.RemoteEntry.LastLocalSyncTsNs/1e9 < entry.Attributes.Mtime {
			return true // should not uncache an entry that is not synchronized with remote
		}

		entry.RemoteEntry.LastLocalSyncTsNs = 0
		entry.Chunks = nil

		fmt.Fprintf(writer, "Uncache %+v ... ", dir.Child(entry.Name))

		err := commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			_, updateErr := client.UpdateEntry(context.Background(), &filer_pb.UpdateEntryRequest{
				Directory: string(dir),
				Entry:     entry,
			})
			return updateErr
		})
		if err != nil {
			fmt.Fprintf(writer, "uncache %+v: %v\n", dir.Child(entry.Name), err)
			return false
		}
		fmt.Fprintf(writer, "Done\n")

		return true
	})
}

type FileFilter struct {
	include     *string
	exclude     *string
	minSize     *int64
	maxSize     *int64
	minAge      *int64
	maxAge      *int64
	minCacheAge *int64
	now         int64
}

func newFileFilter(remoteMountCommand *flag.FlagSet) (ff *FileFilter) {
	ff = &FileFilter{}
	ff.include = remoteMountCommand.String("include", "", "patterns of file names, e.g., *.pdf, *.html, ab?d.txt")
	ff.exclude = remoteMountCommand.String("exclude", "", "patterns of file names, e.g., *.pdf, *.html, ab?d.txt")
	ff.minSize = remoteMountCommand.Int64("minSize", -1, "minimum file size in bytes")
	ff.maxSize = remoteMountCommand.Int64("maxSize", -1, "maximum file size in bytes")
	ff.minAge = remoteMountCommand.Int64("minAge", -1, "minimum file age in seconds (created time)")
	ff.maxAge = remoteMountCommand.Int64("maxAge", -1, "maximum file age in seconds (created time)")
	ff.minCacheAge = remoteMountCommand.Int64("minCacheAge", -1, "minimum file cache age in seconds (last cached time)")
	ff.now = time.Now().Unix()
	return
}

// matchesName applies only the name-based include/exclude patterns,
// usable for remote entries where local attributes are not available.
func (ff *FileFilter) matchesName(name string) bool {
	if *ff.include != "" {
		if ok, _ := filepath.Match(*ff.include, name); !ok {
			return false
		}
	}
	if *ff.exclude != "" {
		if ok, _ := filepath.Match(*ff.exclude, name); ok {
			return false
		}
	}
	return true
}

func (ff *FileFilter) matches(entry *filer_pb.Entry) bool {
	if entry.Attributes == nil {
		return false
	}
	if !ff.matchesName(entry.Name) {
		return false
	}
	if *ff.minSize != -1 {
		if int64(entry.Attributes.FileSize) < *ff.minSize {
			return false
		}
	}
	if *ff.maxSize != -1 {
		if int64(entry.Attributes.FileSize) > *ff.maxSize {
			return false
		}
	}
	if *ff.minAge != -1 {
		if entry.Attributes.Crtime+*ff.minAge > ff.now {
			return false
		}
	}
	if *ff.maxAge != -1 {
		if entry.Attributes.Crtime+*ff.maxAge < ff.now {
			return false
		}
	}
	if *ff.minCacheAge != -1 {
		lastCachedTime := entry.Attributes.Crtime
		if entry.RemoteEntry != nil && entry.RemoteEntry.LastLocalSyncTsNs > 0 {
			lastCachedTime = entry.RemoteEntry.LastLocalSyncTsNs / 1e9
		}
		if lastCachedTime+*ff.minCacheAge > ff.now {
			return false
		}
	}
	return true
}
