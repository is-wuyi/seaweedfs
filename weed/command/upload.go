package command

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	upload UploadOptions
)

type UploadOptions struct {
	master       *string
	dir          *string
	include      *string
	replication  *string
	collection   *string
	dataCenter   *string
	ttl          *string
	diskType     *string
	maxMB        *int
	usePublicUrl *bool
}

func init() {
	cmdUpload.Run = runUpload // break init cycle
	cmdUpload.IsDebug = cmdUpload.Flag.Bool("debug", false, "verbose debug information")
	upload.master = cmdUpload.Flag.String("master", "localhost:9333", "SeaweedFS master location")
	upload.dir = cmdUpload.Flag.String("dir", "", "Upload the whole folder recursively if specified.")
	upload.include = cmdUpload.Flag.String("include", "", "patterns of files to upload, e.g., *.pdf, *.html, ab?d.txt, works together with -dir")
	upload.replication = cmdUpload.Flag.String("replication", "", "replication type")
	upload.collection = cmdUpload.Flag.String("collection", "", "optional collection name")
	upload.dataCenter = cmdUpload.Flag.String("dataCenter", "", "optional data center name")
	upload.diskType = cmdUpload.Flag.String("disk", "", "[hdd|ssd|<tag>] hard drive or solid state drive or any tag")
	upload.ttl = cmdUpload.Flag.String("ttl", "", "time to live, e.g.: 1m, 1h, 1d, 1M, 1y")
	upload.maxMB = cmdUpload.Flag.Int("maxMB", 4, "split files larger than the limit")
	upload.usePublicUrl = cmdUpload.Flag.Bool("usePublicUrl", false, "upload to public url from volume server")
}

var cmdUpload = &Command{
	UsageLine: "upload -master=localhost:9333 file1 [file2 file3]\n         weed upload -master=localhost:9333 -dir=one_directory -include=*.pdf",
	Short:     "上传一个或一组文件",
	Long: `上传一个或一组文件,或递归批量上传整个文件夹。

  如果上传一组文件:
  会为这组文件使用连续的文件键。
  例如,如果 file1 使用键 k,则 file2 可通过 k_1 读取

  如果递归上传整个文件夹:
  文件夹及子文件夹下的所有文件都会被上传,每个文件都有自己的文件键。
  可选参数 "-include" 允许你指定文件名模式。

  如果 "maxMB" 设置为正数,大于该值的文件会被切分成 chunk 并分别上传。
  这些 chunk 的文件 ID 列表会存储在一个额外的 chunk 中,并返回这个额外 chunk 的文件 ID。

  `,
}

func runUpload(cmd *Command, args []string) bool {

	util.LoadSecurityConfiguration()
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")

	defaultReplication, err := readMasterConfiguration(grpcDialOption, pb.ServerAddress(*upload.master))
	if err != nil {
		fmt.Printf("upload: %v", err)
		return false
	}
	if *upload.replication == "" {
		*upload.replication = defaultReplication
	}

	if len(args) == 0 {
		if *upload.dir == "" {
			return false
		}
		err = filepath.Walk(util.ResolvePath(*upload.dir), func(path string, info os.FileInfo, err error) error {
			if err == nil {
				if !info.IsDir() {
					if *upload.include != "" {
						if ok, _ := filepath.Match(*upload.include, filepath.Base(path)); !ok {
							return nil
						}
					}
					parts, e := operation.NewFileParts([]string{path})
					if e != nil {
						return e
					}
					results, e := operation.SubmitFiles(func(_ context.Context) pb.ServerAddress { return pb.ServerAddress(*upload.master) }, grpcDialOption, parts, operation.StoragePreference{
						Replication: *upload.replication,
						Collection:  *upload.collection,
						DataCenter:  *upload.dataCenter,
						Ttl:         *upload.ttl,
						DiskType:    *upload.diskType,
						MaxMB:       *upload.maxMB,
					}, *upload.usePublicUrl)
					bytes, _ := json.Marshal(results)
					fmt.Println(string(bytes))
					if e != nil {
						return e
					}
				}
			} else {
				fmt.Println(err)
			}
			return err
		})
		if err != nil {
			fmt.Println(err.Error())
			return false
		}
	} else {
		parts, e := operation.NewFileParts(args)
		if e != nil {
			fmt.Println(e.Error())
			return false
		}
		results, err := operation.SubmitFiles(func(_ context.Context) pb.ServerAddress { return pb.ServerAddress(*upload.master) }, grpcDialOption, parts, operation.StoragePreference{
			Replication: *upload.replication,
			Collection:  *upload.collection,
			DataCenter:  *upload.dataCenter,
			Ttl:         *upload.ttl,
			DiskType:    *upload.diskType,
			MaxMB:       *upload.maxMB,
		}, *upload.usePublicUrl)
		if err != nil {
			fmt.Println(err.Error())
			return false
		}
		bytes, _ := json.Marshal(results)
		fmt.Println(string(bytes))
	}
	return true
}

func readMasterConfiguration(grpcDialOption grpc.DialOption, masterAddress pb.ServerAddress) (replication string, err error) {
	err = pb.WithMasterClient(context.Background(), false, masterAddress, grpcDialOption, false, func(client master_pb.SeaweedClient) error {
		resp, err := client.GetMasterConfiguration(context.Background(), &master_pb.GetMasterConfigurationRequest{})
		if err != nil {
			return fmt.Errorf("get master %s configuration: %v", masterAddress, err)
		}
		replication = resp.DefaultReplication
		return nil
	})
	return
}
