package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	Commands = append(Commands, &commandS3BucketOwner{})
}

type commandS3BucketOwner struct {
}

func (c *commandS3BucketOwner) Name() string {
	return "s3.bucket.owner"
}

func (c *commandS3BucketOwner) Help() string {
	return `查看或更改 S3 bucket 的所有者

	示例：
		# 查看 bucket 当前的所有者
		s3.bucket.owner -name <bucket_name>

		# 设置或更改 bucket 的所有者
		s3.bucket.owner -name <bucket_name> -owner <identity_name>

		# 移除所有者（使 bucket 仅限管理员访问）
		s3.bucket.owner -name <bucket_name> -delete

	所有者身份决定了哪个 S3 用户可以访问该 bucket。非管理员用户只能访问他们
	拥有的 bucket。管理员用户可以访问所有 bucket，无论所有者是谁。

	-owner 的值应与 S3 IAM 系统中配置的身份名称匹配（即 s3.json identities
	配置中的 "name" 字段）。
`
}

func (c *commandS3BucketOwner) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3BucketOwner) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	bucketCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	bucketName := bucketCommand.String("name", "", "bucket name")
	bucketOwner := bucketCommand.String("owner", "", "new bucket owner identity name")
	deleteOwner := bucketCommand.Bool("delete", false, "remove the bucket owner (make admin-only)")
	if err = bucketCommand.Parse(args); err != nil {
		return nil
	}

	if *bucketName == "" {
		return fmt.Errorf("empty bucket name")
	}

	// Trim whitespace from owner
	owner := strings.TrimSpace(*bucketOwner)

	// Validate flags: can't use both -owner and -delete
	if owner != "" && *deleteOwner {
		return fmt.Errorf("cannot use both -owner and -delete flags together")
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

		// If -owner is provided, set the owner
		if owner != "" {
			if entry.Extended == nil {
				entry.Extended = make(map[string][]byte)
			}
			entry.Extended[s3_constants.AmzIdentityId] = []byte(owner)
			fmt.Fprintf(writer, "Setting owner of bucket %s to: %s\n", *bucketName, owner)

			// Update the entry
			if _, err := client.UpdateEntry(context.Background(), &filer_pb.UpdateEntryRequest{
				Directory: filerBucketsPath,
				Entry:     entry,
			}); err != nil {
				return fmt.Errorf("failed to update bucket: %w", err)
			}

			fmt.Fprintf(writer, "Bucket owner updated successfully.\n")
			return nil
		}

		// If -delete is provided, remove the owner
		if *deleteOwner {
			if entry.Extended != nil {
				delete(entry.Extended, s3_constants.AmzIdentityId)
			}
			fmt.Fprintf(writer, "Removing owner from bucket %s\n", *bucketName)

			// Update the entry
			if _, err := client.UpdateEntry(context.Background(), &filer_pb.UpdateEntryRequest{
				Directory: filerBucketsPath,
				Entry:     entry,
			}); err != nil {
				return fmt.Errorf("failed to update bucket: %w", err)
			}

			fmt.Fprintf(writer, "Bucket owner removed. Bucket is now admin-only.\n")
			return nil
		}

		// Display current owner (no flags provided)
		fmt.Fprintf(writer, "Bucket: %s\n", *bucketName)
		fmt.Fprintf(writer, "Path: %s\n", util.NewFullPath(filerBucketsPath, *bucketName))

		if entry.Extended != nil {
			if ownerBytes, ok := entry.Extended[s3_constants.AmzIdentityId]; ok && len(ownerBytes) > 0 {
				fmt.Fprintf(writer, "Owner: %s\n", string(ownerBytes))
			} else {
				fmt.Fprintf(writer, "Owner: (none - admin access only)\n")
			}
		} else {
			fmt.Fprintf(writer, "Owner: (none - admin access only)\n")
		}

		return nil
	})

	return err
}
