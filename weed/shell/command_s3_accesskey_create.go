package shell

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/iam"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

func init() {
	Commands = append(Commands, &commandS3AccessKeyCreate{})
}

type commandS3AccessKeyCreate struct {
}

func (c *commandS3AccessKeyCreate) Name() string {
	return "s3.accesskey.create"
}

func (c *commandS3AccessKeyCreate) Help() string {
	return `为 S3 IAM 用户创建额外的访问密钥

	s3.accesskey.create -user <username>
	s3.accesskey.create -user <username> -access_key <key> -secret_key <secret>

	为已有用户生成新的凭证对。如果省略 -access_key 和 -secret_key，将自动生成。
`
}

func (c *commandS3AccessKeyCreate) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3AccessKeyCreate) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	f := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	user := f.String("user", "", "user name")
	accessKey := f.String("access_key", "", "access key (generated if omitted)")
	secretKey := f.String("secret_key", "", "secret key (generated if omitted)")
	if err := f.Parse(args); err != nil {
		return err
	}

	if *user == "" {
		return fmt.Errorf("-user is required")
	}

	ak := *accessKey
	sk := *secretKey

	if ak == "" && sk == "" {
		var err error
		ak, err = iam.GenerateRandomString(iam.AccessKeyIdLength, iam.CharsetUpper)
		if err != nil {
			return fmt.Errorf("generate access key: %v", err)
		}
		sk, err = iam.GenerateSecretAccessKey()
		if err != nil {
			return fmt.Errorf("generate secret key: %v", err)
		}
	} else if ak == "" || sk == "" {
		return fmt.Errorf("both -access_key and -secret_key must be provided together, or omit both to auto-generate")
	}

	err := commandEnv.withIamClient(func(ctx context.Context, client iam_pb.SeaweedIdentityAccessManagementClient) error {
		_, err := client.CreateAccessKey(ctx, &iam_pb.CreateAccessKeyRequest{
			Username: *user,
			Credential: &iam_pb.Credential{
				AccessKey: ak,
				SecretKey: sk,
				Status:    iam.AccessKeyStatusActive,
			},
		})
		return err
	})
	if err != nil {
		return err
	}

	fmt.Fprintf(writer, "Created access key for user %q\n", *user)
	fmt.Fprintf(writer, "Access Key: %s\n", ak)
	fmt.Fprintf(writer, "Secret Key: %s\n", sk)
	fmt.Fprintln(writer)
	fmt.Fprintln(writer, "Save these credentials - the secret key cannot be retrieved later.")
	return nil
}
