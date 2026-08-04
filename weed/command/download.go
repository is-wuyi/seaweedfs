package command

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

var (
	d DownloadOptions
)

type DownloadOptions struct {
	master *string
	server *string // deprecated, for backward compatibility
	dir    *string
}

func init() {
	cmdDownload.Run = runDownload // break init cycle
	d.master = cmdDownload.Flag.String("master", "localhost:9333", "SeaweedFS master location")
	d.server = cmdDownload.Flag.String("server", "", "SeaweedFS master location (deprecated, use -master instead)")
	d.dir = cmdDownload.Flag.String("dir", ".", "Download the whole folder recursively if specified.")
}

var cmdDownload = &Command{
	UsageLine: "download -master=localhost:9333 -dir=one_directory fid1 [fid2 fid3 ...]",
	Short:     "按文件 ID 下载文件",
	Long: `按文件 ID 下载文件。

  通常你只需用 curl 查找文件所在的 volume 服务器,然后直接下载即可。
  这个下载工具将这两步合二为一。

  此外,如果你使用 "weed upload -maxMB=..." 选项上传了一个被切分成 chunk 的大文件,你可以
  使用此工具下载这些 chunk 并自动合并。

  `,
}

func runDownload(cmd *Command, args []string) bool {
	util.LoadSecurityConfiguration()
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")

	// Backward compatibility: if -server is provided, use it
	masterServer := *d.master
	if *d.server != "" {
		masterServer = *d.server
	}

	*d.dir = util.ResolvePath(*d.dir)
	for _, fid := range args {
		if e := downloadToFile(func(_ context.Context) pb.ServerAddress { return pb.ServerAddress(masterServer) }, grpcDialOption, fid, *d.dir); e != nil {
			fmt.Println("Download Error: ", fid, e)
		}
	}
	return true
}

func downloadToFile(masterFn operation.GetMasterFn, grpcDialOption grpc.DialOption, fileId, saveDir string) error {
	fileUrl, jwt, lookupError := operation.LookupFileId(masterFn, grpcDialOption, fileId)
	if lookupError != nil {
		return lookupError
	}
	filename, _, rc, err := util_http.DownloadFile(fileUrl, jwt)
	if err != nil {
		return err
	}
	defer util_http.CloseResponse(rc)
	if filename == "" {
		filename = fileId
	}
	isFileList := false
	if strings.HasSuffix(filename, "-list") {
		// old command compatible
		isFileList = true
		filename = filename[0 : len(filename)-len("-list")]
	}
	f, err := os.OpenFile(path.Join(saveDir, filename), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return err
	}
	defer f.Close()
	if isFileList {
		content, err := io.ReadAll(rc.Body)
		if err != nil {
			return err
		}
		fids := strings.Split(string(content), "\n")
		for _, partId := range fids {
			var n int
			_, part, err := fetchContent(masterFn, grpcDialOption, partId)
			if err == nil {
				n, err = f.Write(part)
			}
			if err == nil && n < len(part) {
				err = io.ErrShortWrite
			}
			if err != nil {
				return err
			}
		}
	} else {
		if _, err = io.Copy(f, rc.Body); err != nil {
			return err
		}

	}
	return nil
}

func fetchContent(masterFn operation.GetMasterFn, grpcDialOption grpc.DialOption, fileId string) (filename string, content []byte, e error) {
	fileUrl, jwt, lookupError := operation.LookupFileId(masterFn, grpcDialOption, fileId)
	if lookupError != nil {
		return "", nil, lookupError
	}
	var rc *http.Response
	if filename, _, rc, e = util_http.DownloadFile(fileUrl, jwt); e != nil {
		return "", nil, e
	}
	defer util_http.CloseResponse(rc)
	content, e = io.ReadAll(rc.Body)
	return
}
