package command

import (
	"fmt"
	"path/filepath"

	"github.com/seaweedfs/seaweedfs/weed/util"

	"github.com/seaweedfs/seaweedfs/weed/command/scaffold"
)

func init() {
	cmdScaffold.Run = runScaffold // break init cycle
}

var cmdScaffold = &Command{
	UsageLine: "scaffold -config=[filer|notification|replication|security|master|volume|shell|credential|admin]",
	Short:     "生成基础配置文件",
	Long: `生成配置文件,包含所有可能的配置项供你自定义。

	这些选项也可以通过环境变量覆盖。
	例如,filer.toml 的 mysql 密码可以通过环境变量覆盖
		export WEED_MYSQL_PASSWORD=some_password
	环境变量规则:
		* 在变量名前加 "WEED_" 前缀。
		* 变量名其余部分大写。
		* 将 '.' 替换为 '_'。

  `,
}

var (
	outputPath = cmdScaffold.Flag.String("output", "", "if not empty, save the configuration file to this directory")
	config     = cmdScaffold.Flag.String("config", "filer", "[filer|notification|replication|security|master|volume|shell|credential|admin] the configuration file to generate")
)

func runScaffold(cmd *Command, args []string) bool {

	content := ""
	switch *config {
	case "filer":
		content = scaffold.Filer
	case "notification":
		content = scaffold.Notification
	case "replication":
		content = scaffold.Replication
	case "security":
		content = scaffold.Security
	case "master":
		content = scaffold.Master
	case "volume":
		content = scaffold.Volume
	case "shell":
		content = scaffold.Shell
	case "credential":
		content = scaffold.Credential
	case "admin":
		content = scaffold.Admin
	}
	if content == "" {
		println("need a valid -config option")
		return false
	}

	if *outputPath != "" {
		util.WriteFile(filepath.Join(util.ResolvePath(*outputPath), *config+".toml"), []byte(content), 0644)
	} else {
		fmt.Println(content)
	}
	return true
}
