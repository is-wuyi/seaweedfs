package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	Commands = append(Commands, &commandRemoteCopyLocal{})
}

type commandRemoteCopyLocal struct {
}

func (c *commandRemoteCopyLocal) Name() string {
	return "remote.copy.local"
}

func (c *commandRemoteCopyLocal) Help() string {
	return `将本地文件复制到远端存储

	# 假设远端存储已配置为名称 "cloud1"
	remote.configure -name=cloud1 -type=s3 -s3.access_key=xxx -s3.secret_key=yyy
	# 挂载并拉取一个 bucket
	remote.mount -dir=/xxx -remote=cloud1/bucket

	# 将本地文件复制到远端存储
	remote.copy.local -dir=/xxx                                    # 复制所有本地独有的文件
	remote.copy.local -dir=/xxx -concurrent=16                     # 自定义并发数
	remote.copy.local -dir=/xxx -include=*.pdf                     # 仅复制 PDF 文件
	remote.copy.local -dir=/xxx -exclude=*.tmp                     # 排除临时文件
	remote.copy.local -dir=/xxx -dryRun=true                       # 仅显示将要执行的操作，不实际修改
	remote.copy.local -dir=/xxx -forceUpdate=true                  # 即使远端已存在也强制更新
	remote.copy.local -dir=/xxx -delete                            # 同时删除本地不存在的远端文件

	此命令将：
	1. 查找远端存储中不存在的本地文件
	2. 将这些文件复制到远端存储
	3. 用远端信息更新本地元数据
	4. 使用 -delete 时，删除本地不存在的远端文件（类似 rsync --delete）

	适用于以下场景：
	- 你删除了 filer 日志并需要复制现有文件
	- 你有从未复制到远端的本地文件
	- 你想确保所有本地文件都备份到远端
	- 你想进行计划性的一次性备份并同步本地删除（使用 -delete）

	关于 -delete 的说明：
	- 仅考虑 -dir 下的远端文件；不会删除该路径之外的文件
	- -include/-exclude 模式也会限制要删除的远端文件
	- 大小/时间过滤器仅适用于复制，不适用于删除
	- 先使用 -dryRun=true 查看将要删除的内容

 `
}

func (c *commandRemoteCopyLocal) HasTag(CommandTag) bool {
	return false
}

func (c *commandRemoteCopyLocal) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	remoteCopyLocalCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)

	dir := remoteCopyLocalCommand.String("dir", "", "a directory in filer")
	concurrency := remoteCopyLocalCommand.Int("concurrent", 16, "concurrent file operations")
	dryRun := remoteCopyLocalCommand.Bool("dryRun", false, "show what would be done without making changes")
	forceUpdate := remoteCopyLocalCommand.Bool("forceUpdate", false, "force update even if remote exists")
	deleteExtraneous := remoteCopyLocalCommand.Bool("delete", false, "delete extraneous files from remote storage (files that do not exist locally), similar to rsync --delete")
	fileFilter := newFileFilter(remoteCopyLocalCommand)

	if err = remoteCopyLocalCommand.Parse(args); err != nil {
		return err
	}

	if *dir == "" {
		return fmt.Errorf("need to specify -dir option")
	}

	mappings, localMountedDir, remoteStorageMountedLocation, remoteStorageConf, detectErr := detectMountInfo(commandEnv, writer, *dir)
	if detectErr != nil {
		jsonPrintln(writer, mappings)
		return detectErr
	}

	// perform local to remote copy
	return c.doLocalToRemoteCopy(commandEnv, writer, util.FullPath(localMountedDir), remoteStorageMountedLocation, util.FullPath(*dir), remoteStorageConf, *concurrency, *dryRun, *forceUpdate, *deleteExtraneous, fileFilter)
}

func (c *commandRemoteCopyLocal) doLocalToRemoteCopy(commandEnv *CommandEnv, writer io.Writer, localMountedDir util.FullPath, remoteMountedLocation *remote_pb.RemoteStorageLocation, dirToCopy util.FullPath, remoteConf *remote_pb.RemoteConf, concurrency int, dryRun bool, forceUpdate bool, deleteExtraneous bool, fileFilter *FileFilter) error {

	// Get remote storage client
	remoteStorage, err := remote_storage.GetRemoteStorage(remoteConf)
	if err != nil {
		return fmt.Errorf("failed to get remote storage: %w", err)
	}

	remote := filer.MapFullPathToRemoteStorageLocation(localMountedDir, remoteMountedLocation, dirToCopy)

	// Step 1: Collect all local files that are part of the remote mount
	localFiles := make(map[string]*filer_pb.Entry)
	err = recursivelyTraverseDirectory(commandEnv, dirToCopy, func(dir util.FullPath, entry *filer_pb.Entry) bool {
		// Only consider files that are part of remote mount
		if isInMountedDirectory(dir, localMountedDir) {
			fullPath := string(dir.Child(entry.Name))
			localFiles[fullPath] = entry
		}
		return true
	})
	if err != nil {
		return fmt.Errorf("failed to traverse local directory: %w", err)
	}

	fmt.Fprintf(writer, "Found %d files/directories in local storage\n", len(localFiles))

	// Step 2: Check which files exist on remote storage
	remoteFiles := make(map[string]bool) // full path -> isDirectory
	err = remoteStorage.Traverse(remote, func(remoteDir, name string, isDirectory bool, remoteEntry *filer_pb.RemoteEntry) error {
		localDir := filer.MapRemoteStorageLocationPathToFullPath(localMountedDir, remoteMountedLocation, remoteDir)
		fullPath := string(localDir.Child(name))
		remoteFiles[fullPath] = isDirectory
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to traverse remote storage: %w", err)
	}

	fmt.Fprintf(writer, "Found %d files/directories in remote storage\n", len(remoteFiles))

	// Step 3: Determine files to copy and files to delete
	plan := planLocalToRemoteSync(localFiles, remoteFiles, dirToCopy, forceUpdate, deleteExtraneous, fileFilter)

	fmt.Fprintf(writer, "Files to copy: %d\n", len(plan.filesToCopy))
	if deleteExtraneous {
		fmt.Fprintf(writer, "Files to delete from remote: %d\n", len(plan.filesToDelete))
	}

	if dryRun {
		fmt.Fprintf(writer, "DRY RUN - showing what would be done:\n")
		for _, path := range plan.filesToCopy {
			fmt.Fprintf(writer, "COPY: %s\n", path)
		}
		for _, path := range plan.filesToDelete {
			fmt.Fprintf(writer, "DELETE: %s\n", path)
		}
		return nil
	}

	if len(plan.filesToCopy) == 0 && len(plan.filesToDelete) == 0 {
		fmt.Fprintf(writer, "No files to copy\n")
		return nil
	}

	var wg sync.WaitGroup
	limitedConcurrentExecutor := util.NewLimitedConcurrentExecutor(concurrency)
	var firstErr error
	var errOnce sync.Once
	var successCount atomic.Int64
	var outputMu sync.Mutex

	// Step 4: Copy files to remote storage
	for _, pathToCopy := range plan.filesToCopy {
		wg.Add(1)
		localPath := pathToCopy // Capture for closure
		limitedConcurrentExecutor.Execute(func() {
			defer wg.Done()

			localEntry := localFiles[localPath]
			if localEntry == nil {
				outputMu.Lock()
				fmt.Fprintf(writer, "Warning: skipping copy for %s (local entry not found)\n", localPath)
				outputMu.Unlock()
				return
			}

			outputMu.Lock()
			fmt.Fprintf(writer, "Copying %s... ", localPath)
			outputMu.Unlock()

			dir, _ := util.FullPath(localPath).DirAndName()
			remoteLocation := filer.MapFullPathToRemoteStorageLocation(localMountedDir, remoteMountedLocation, util.FullPath(localPath))

			// Copy the file to remote storage
			err := syncFileToRemote(commandEnv, remoteStorage, remoteConf, remoteLocation, util.FullPath(dir), localEntry)
			if err != nil {
				outputMu.Lock()
				fmt.Fprintf(writer, "failed: %v\n", err)
				outputMu.Unlock()
				errOnce.Do(func() {
					firstErr = err
				})
				return
			}

			successCount.Add(1)
			outputMu.Lock()
			fmt.Fprintf(writer, "done\n")
			outputMu.Unlock()
		})
	}

	wg.Wait()
	if firstErr != nil {
		// skip deletion when any copy failed, to stay on the safe side
		return firstErr
	}

	if len(plan.filesToCopy) > 0 {
		fmt.Fprintf(writer, "Successfully copied %d files to remote storage\n", successCount.Load())
	}

	// Step 5: Delete extraneous files from remote storage
	if len(plan.filesToDelete) == 0 {
		return nil
	}

	var deleteErr error
	var deleteErrOnce sync.Once
	var deletedCount atomic.Int64

	for _, pathToDelete := range plan.filesToDelete {
		wg.Add(1)
		localPath := pathToDelete // Capture for closure
		limitedConcurrentExecutor.Execute(func() {
			defer wg.Done()

			remoteLocation := filer.MapFullPathToRemoteStorageLocation(localMountedDir, remoteMountedLocation, util.FullPath(localPath))
			if err := remoteStorage.DeleteFile(remoteLocation); err != nil {
				outputMu.Lock()
				fmt.Fprintf(writer, "Deleting %s... failed: %v\n", localPath, err)
				outputMu.Unlock()
				deleteErrOnce.Do(func() {
					deleteErr = err
				})
				return
			}

			deletedCount.Add(1)
			outputMu.Lock()
			fmt.Fprintf(writer, "Deleting %s... done\n", localPath)
			outputMu.Unlock()
		})
	}
	wg.Wait()

	if deleteErr != nil {
		return deleteErr
	}

	fmt.Fprintf(writer, "Successfully deleted %d files from remote storage\n", deletedCount.Load())
	return nil
}

type localToRemoteSyncPlan struct {
	filesToCopy   []string
	filesToDelete []string
}

func planLocalToRemoteSync(localFiles map[string]*filer_pb.Entry, remoteFiles map[string]bool, dirToCopy util.FullPath, forceUpdate bool, deleteExtraneous bool, fileFilter *FileFilter) *localToRemoteSyncPlan {
	plan := &localToRemoteSyncPlan{}

	for localPath, localEntry := range localFiles {
		// Skip directories
		if localEntry.IsDirectory {
			continue
		}

		// Apply file filter
		if !fileFilter.matches(localEntry) {
			continue
		}

		// Copy if the file doesn't exist on remote, or if force update is requested
		if _, foundOnRemote := remoteFiles[localPath]; !foundOnRemote || forceUpdate {
			plan.filesToCopy = append(plan.filesToCopy, localPath)
		}
	}
	sort.Strings(plan.filesToCopy)

	if !deleteExtraneous {
		return plan
	}

	for remotePath, isDirectory := range remoteFiles {
		// Traverse lists by key prefix (no delimiter), so it can return entries
		// outside dirToCopy that merely share its name prefix (e.g. a sibling
		// "foobar" when dirToCopy maps to prefix "foo"). Never delete anything
		// outside the requested subtree.
		if !util.FullPath(remotePath).IsUnder(dirToCopy) {
			continue
		}
		if _, foundLocally := localFiles[remotePath]; foundLocally {
			continue
		}
		// object stores expose no real directories; only files are deleted
		if isDirectory {
			continue
		}
		// name filters also protect remote files from deletion;
		// size/age filters need local attributes, so they only apply to copying
		if !fileFilter.matchesName(util.FullPath(remotePath).Name()) {
			continue
		}
		plan.filesToDelete = append(plan.filesToDelete, remotePath)
	}
	sort.Strings(plan.filesToDelete)

	return plan
}

func syncFileToRemote(commandEnv *CommandEnv, remoteStorage remote_storage.RemoteStorageClient, remoteConf *remote_pb.RemoteConf, remoteLocation *remote_pb.RemoteStorageLocation, dir util.FullPath, localEntry *filer_pb.Entry) error {

	// Upload to remote storage using the same approach as filer_remote_sync
	var remoteEntry *filer_pb.RemoteEntry
	var err error

	err = util.Retry("writeFile", func() error {
		// Create a reader for the file content
		reader := filer.NewFileReader(commandEnv, localEntry)

		remoteEntry, err = remoteStorage.WriteFile(remoteLocation, localEntry, reader)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to upload to remote storage: %w", err)
	}

	// Update local entry with remote information
	return commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		ctx := context.Background()

		// Update local entry with remote information
		remoteEntry.LastLocalSyncTsNs = time.Now().UnixNano()
		localEntry.RemoteEntry = remoteEntry

		// Update the entry
		_, updateErr := client.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{
			Directory: string(dir),
			Entry:     localEntry,
		})
		if updateErr != nil {
			return fmt.Errorf("failed to update local entry: %w", updateErr)
		}

		return nil
	})
}

func isInMountedDirectory(dir util.FullPath, mountedDir util.FullPath) bool {
	if string(dir) == string(mountedDir) {
		return true
	}
	// Ensure mountedDir ends with separator to avoid matching sibling directories
	// e.g., "/mnt/remote2" should not match "/mnt/remote"
	mountedDirStr := string(mountedDir)
	if !strings.HasSuffix(mountedDirStr, "/") {
		mountedDirStr += "/"
	}
	return strings.HasPrefix(string(dir)+"/", mountedDirStr)
}
