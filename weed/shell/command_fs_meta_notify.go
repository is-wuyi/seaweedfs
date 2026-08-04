package shell

import (
	"context"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/notification"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	Commands = append(Commands, &commandFsMetaNotify{})
}

type commandFsMetaNotify struct {
}

func (c *commandFsMetaNotify) Name() string {
	return "fs.meta.notify"
}

func (c *commandFsMetaNotify) Help() string {
	return `递归发送目录和文件的元数据到通知消息队列

	fs.meta.notify	# 从当前目录发送元数据到通知消息队列

	消息队列将使用它来触发从此 filer 的副本同步。

`
}

func (c *commandFsMetaNotify) HasTag(CommandTag) bool {
	return false
}

func (c *commandFsMetaNotify) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	if handleHelpRequest(c, args, writer) {
		return nil
	}

	path, err := commandEnv.parseUrl(findInputDirectory(args))
	if err != nil {
		return err
	}

	util.LoadConfiguration("notification", true)
	v := util.GetViper()
	notification.LoadConfiguration(v, "notification.")

	var dirCount, fileCount uint64

	err = filer_pb.TraverseBfs(context.Background(), commandEnv, util.FullPath(path), func(parentPath util.FullPath, entry *filer_pb.Entry) error {

		if entry.IsDirectory {
			dirCount++
		} else {
			fileCount++
		}

		notifyErr := notification.Queue.SendMessage(
			string(parentPath.Child(entry.Name)),
			&filer_pb.EventNotification{
				NewEntry: entry,
			},
		)

		if notifyErr != nil {
			fmt.Fprintf(writer, "fail to notify new entry event for %s: %v\n", parentPath.Child(entry.Name), notifyErr)
		}
		return nil
	})

	if err == nil {
		fmt.Fprintf(writer, "\ntotal notified %d directories, %d files\n", dirCount, fileCount)
	}

	return err

}
