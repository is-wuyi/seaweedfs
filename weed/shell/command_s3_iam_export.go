package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	Commands = append(Commands, &commandS3IAMExport{})
}

type commandS3IAMExport struct {
}

func (c *commandS3IAMExport) Name() string {
	return "s3.iam.export"
}

func (c *commandS3IAMExport) Help() string {
	return `以 JSON 格式导出完整的 S3 IAM 配置

	s3.iam.export
	s3.iam.export -file backup.json

	导出所有用户、凭证、策略、服务账号和组。不指定 -file 时输出到 stdout。
`
}

func (c *commandS3IAMExport) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3IAMExport) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	f := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	file := f.String("file", "", "output file path (stdout if omitted)")
	if err := f.Parse(args); err != nil {
		return err
	}

	return commandEnv.withIamClient(func(ctx context.Context, client iam_pb.SeaweedIdentityAccessManagementClient) error {
		resp, err := client.GetConfiguration(ctx, &iam_pb.GetConfigurationRequest{})
		if err != nil {
			return err
		}

		var out io.Writer = writer
		outputFile := util.ResolvePath(*file)
		if outputFile != "" {
			fp, err := os.OpenFile(outputFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
			if err != nil {
				return fmt.Errorf("create file: %v", err)
			}
			defer fp.Close()
			out = fp
		}

		if err := filer.ProtoToText(out, resp.Configuration); err != nil {
			return err
		}
		fmt.Fprintln(out)

		if outputFile != "" {
			fmt.Fprintf(writer, "Exported IAM configuration to %s\n", outputFile)
		}
		return nil
	})
}
