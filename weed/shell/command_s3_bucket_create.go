package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3bucket"
)

func init() {
	Commands = append(Commands, &commandS3BucketCreate{})
}

type commandS3BucketCreate struct {
}

func (c *commandS3BucketCreate) Name() string {
	return "s3.bucket.create"
}

func (c *commandS3BucketCreate) Help() string {
	return `使用给定名称创建一个 bucket

	示例：
		s3.bucket.create -name <bucket_name>
		s3.bucket.create -name <bucket_name> -owner <identity_name>
		s3.bucket.create -name <bucket_name> -withLock

	-owner 标志用于设置 bucket 的所有者身份。在使用 S3 IAM 身份验证时这很重要，
	因为非管理员用户只能访问他们拥有的 bucket。如果未指定，bucket 将没有所有者，
	只能由管理员用户访问。

	-owner 的值应与 S3 IAM 系统中配置的身份名称匹配（即 s3.json identities
	配置中的 "name" 字段）。

	-withLock 标志会在 bucket 上启用 S3 Object Lock。这为对象提供 WORM
	（Write Once Read Many，一次写入多次读取）保护。一旦启用，Object Lock
	无法被禁用。使用此标志时会自动启用版本控制。
`
}

func (c *commandS3BucketCreate) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3BucketCreate) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	bucketCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	bucketName := bucketCommand.String("name", "", "bucket name")
	bucketOwner := bucketCommand.String("owner", "", "bucket owner identity name (for S3 IAM authentication)")
	withLock := bucketCommand.Bool("withLock", false, "enable Object Lock on the bucket (requires and enables versioning)")
	if err = bucketCommand.Parse(args); err != nil {
		return nil
	}

	if *bucketName == "" {
		return fmt.Errorf("empty bucket name")
	}

	err = s3bucket.VerifyS3BucketName(*bucketName)
	if err != nil {
		return err
	}

	// Trim whitespace from owner and treat whitespace-only as empty
	owner := strings.TrimSpace(*bucketOwner)

	err = commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
		if err != nil {
			return fmt.Errorf("get filer configuration: %w", err)
		}
		filerBucketsPath := resp.DirBuckets

		fmt.Fprintln(writer, "create bucket under", filerBucketsPath)

		entry := &filer_pb.Entry{
			Name:        *bucketName,
			IsDirectory: true,
			Attributes: &filer_pb.FuseAttributes{
				Mtime:    time.Now().Unix(),
				Crtime:   time.Now().Unix(),
				FileMode: uint32(0777 | os.ModeDir),
			},
		}

		// Set bucket owner if specified
		if owner != "" {
			if entry.Extended == nil {
				entry.Extended = make(map[string][]byte)
			}
			entry.Extended[s3_constants.AmzIdentityId] = []byte(owner)
		}

		// Enable Object Lock if specified
		if *withLock {
			if entry.Extended == nil {
				entry.Extended = make(map[string][]byte)
			}
			// Enable versioning (required for Object Lock)
			entry.Extended[s3_constants.ExtVersioningKey] = []byte(s3_constants.VersioningEnabled)
			// Enable Object Lock
			entry.Extended[s3_constants.ExtObjectLockEnabledKey] = []byte(s3_constants.ObjectLockEnabled)
		}

		if _, err := client.CreateEntry(context.Background(), &filer_pb.CreateEntryRequest{
			Directory: filerBucketsPath,
			Entry:     entry,
		}); err != nil {
			return err
		}

		fmt.Fprintln(writer, "created bucket", *bucketName)
		if owner != "" {
			fmt.Fprintln(writer, "bucket owner:", owner)
		}
		if *withLock {
			fmt.Fprintln(writer, "Object Lock: enabled")
			fmt.Fprintln(writer, "Versioning: enabled")
		}

		return nil

	})

	return err

}
