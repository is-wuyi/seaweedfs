package shell

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	Commands = append(Commands, &commandS3BucketLock{})
}

type commandS3BucketLock struct {
}

func (c *commandS3BucketLock) Name() string {
	return "s3.bucket.lock"
}

func (c *commandS3BucketLock) Help() string {
	return `查看或为一个 S3 bucket 启用 Object Lock

	示例：
		# 查看 bucket 当前的 Object Lock 状态
		s3.bucket.lock -name <bucket_name>

		# 为已有的 bucket 启用 Object Lock（不可逆）
		s3.bucket.lock -name <bucket_name> -enable

	Object Lock 为对象提供 WORM（Write Once Read Many，一次写入多次读取）保护。
	一旦启用，无法在 bucket 上禁用 Object Lock。

	要求：
		- 如果尚未启用版本控制，将自动启用版本控制
		- Object Lock 只能启用，不能禁用（遵循 AWS S3 行为）

	启用 Object Lock 后，你可以：
		- 在 bucket 上设置默认保留策略
		- 在单个对象上设置保留和法律保留
`
}

func (c *commandS3BucketLock) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3BucketLock) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	bucketCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	bucketName := bucketCommand.String("name", "", "bucket name")
	enableLock := bucketCommand.Bool("enable", false, "enable Object Lock on the bucket (irreversible)")
	if err = bucketCommand.Parse(args); err != nil {
		return err
	}

	if *bucketName == "" {
		return fmt.Errorf("empty bucket name")
	}

	err = commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
		if err != nil {
			return fmt.Errorf("get filer configuration: %w", err)
		}
		filerBucketsPath := resp.DirBuckets

		// Look up the bucket entry
		lookupResp, err := client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
			Directory: filerBucketsPath,
			Name:      *bucketName,
		})
		if err != nil {
			return fmt.Errorf("lookup bucket %s: %w", *bucketName, err)
		}

		entry := lookupResp.Entry

		// Check current Object Lock status
		currentLockEnabled := false
		currentVersioningEnabled := false
		if entry.Extended != nil {
			if lockStatus, ok := entry.Extended[s3_constants.ExtObjectLockEnabledKey]; ok {
				currentLockEnabled = string(lockStatus) == s3_constants.ObjectLockEnabled
			}
			if versioningStatus, ok := entry.Extended[s3_constants.ExtVersioningKey]; ok {
				currentVersioningEnabled = string(versioningStatus) == s3_constants.VersioningEnabled
			}
		}

		// If -enable is provided, enable Object Lock
		if *enableLock {
			if currentLockEnabled {
				fmt.Fprintf(writer, "Object Lock is already enabled on bucket %s\n", *bucketName)
				return nil
			}

			if entry.Extended == nil {
				entry.Extended = make(map[string][]byte)
			}

			// Enable versioning if not already enabled (required for Object Lock)
			if !currentVersioningEnabled {
				entry.Extended[s3_constants.ExtVersioningKey] = []byte(s3_constants.VersioningEnabled)
				fmt.Fprintf(writer, "Enabling versioning on bucket %s (required for Object Lock)\n", *bucketName)
			}

			// Enable Object Lock
			entry.Extended[s3_constants.ExtObjectLockEnabledKey] = []byte(s3_constants.ObjectLockEnabled)
			fmt.Fprintf(writer, "Enabling Object Lock on bucket %s\n", *bucketName)

			// Update the entry
			if _, err := client.UpdateEntry(context.Background(), &filer_pb.UpdateEntryRequest{
				Directory: filerBucketsPath,
				Entry:     entry,
			}); err != nil {
				return fmt.Errorf("failed to update bucket: %w", err)
			}

			fmt.Fprintf(writer, "Object Lock enabled successfully.\n")
			fmt.Fprintf(writer, "WARNING: This action is irreversible. Object Lock cannot be disabled.\n")
			return nil
		}

		// Display current status (no flags provided)
		fmt.Fprintf(writer, "Bucket: %s\n", *bucketName)
		fmt.Fprintf(writer, "Path: %s\n", util.NewFullPath(filerBucketsPath, *bucketName))

		if currentVersioningEnabled {
			fmt.Fprintf(writer, "Versioning: Enabled\n")
		} else {
			fmt.Fprintf(writer, "Versioning: Disabled\n")
		}

		if currentLockEnabled {
			fmt.Fprintf(writer, "Object Lock: Enabled\n")
		} else {
			fmt.Fprintf(writer, "Object Lock: Disabled\n")
			fmt.Fprintf(writer, "\nTo enable Object Lock, run:\n")
			fmt.Fprintf(writer, "  s3.bucket.lock -name %s -enable\n", *bucketName)
		}

		return nil
	})

	return err
}
