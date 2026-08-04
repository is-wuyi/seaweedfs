package shell

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3bucket"
)

func init() {
	Commands = append(Commands, &commandS3BucketLifecycleFastpath{})
}

type commandS3BucketLifecycleFastpath struct {
}

func (c *commandS3BucketLifecycleFastpath) Name() string {
	return "s3.bucket.lifecycle.fastpath"
}

func (c *commandS3BucketLifecycleFastpath) Help() string {
	return `查看或切换按 bucket 的生命周期 TTL 快速路径

	启用后，Expiration.Days 生命周期规则会在 PutObject 时被标记为 volume TTL，
	这样 volume 服务器会自行回收数据，生命周期 worker 会跳过按 chunk 的删除。
	默认关闭：volume TTL 在写入时固化到对象中，无法遵循后续的策略变更（规则被移除
	或延长），这与 worker 驱动的过期不同。无论此标志如何设置，快速路径都不会
	应用于启用了版本控制或 Object Lock 的 bucket。

	示例：
		# 显示当前设置
		s3.bucket.lifecycle.fastpath -name <bucket_name>

		# 启用
		s3.bucket.lifecycle.fastpath -name <bucket_name> -enable

		# 禁用
		s3.bucket.lifecycle.fastpath -name <bucket_name> -disable
`
}

func (c *commandS3BucketLifecycleFastpath) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3BucketLifecycleFastpath) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	bucketCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	bucketName := bucketCommand.String("name", "", "bucket name")
	enable := bucketCommand.Bool("enable", false, "enable the lifecycle TTL fast path")
	disable := bucketCommand.Bool("disable", false, "disable the lifecycle TTL fast path")
	if err = bucketCommand.Parse(args); err != nil {
		return err
	}

	if *bucketName == "" {
		return fmt.Errorf("empty bucket name")
	}
	if err := s3bucket.VerifyS3BucketName(*bucketName); err != nil {
		return fmt.Errorf("invalid bucket name %q: %w", *bucketName, err)
	}
	if *enable && *disable {
		return fmt.Errorf("only one of -enable or -disable can be set")
	}

	return commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
		if err != nil {
			return fmt.Errorf("get filer configuration: %w", err)
		}
		filerBucketsPath := resp.DirBuckets

		lookupResp, err := client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
			Directory: filerBucketsPath,
			Name:      *bucketName,
		})
		if err != nil {
			return fmt.Errorf("lookup bucket %s: %w", *bucketName, err)
		}
		entry := lookupResp.Entry

		if !*enable && !*disable {
			state := "disabled"
			if string(entry.Extended[s3_constants.ExtLifecycleTtlFastPathKey]) == "true" {
				state = "enabled"
			}
			fmt.Fprintf(writer, "Bucket: %s\n", *bucketName)
			fmt.Fprintf(writer, "Lifecycle TTL fast path: %s\n", state)
			return nil
		}

		if entry.Extended == nil {
			entry.Extended = make(map[string][]byte)
		}
		state := "disabled"
		if *enable {
			entry.Extended[s3_constants.ExtLifecycleTtlFastPathKey] = []byte("true")
			state = "enabled"
		} else {
			delete(entry.Extended, s3_constants.ExtLifecycleTtlFastPathKey)
		}

		if _, err := client.UpdateEntry(context.Background(), &filer_pb.UpdateEntryRequest{
			Directory: filerBucketsPath,
			Entry:     entry,
		}); err != nil {
			return fmt.Errorf("failed to update bucket: %w", err)
		}

		fmt.Fprintf(writer, "Bucket %s lifecycle TTL fast path %s\n", *bucketName, state)
		return nil
	})
}
