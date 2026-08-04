package shell

import (
	"io"
)

func init() {
	Commands = append(Commands, &commandFsCd{})
}

type commandFsCd struct {
}

func (c *commandFsCd) Name() string {
	return "fs.cd"
}

func (c *commandFsCd) Help() string {
	return `切换到指定目录 /path/to/dir

	完整路径可能太长难以输入。例如,
		fs.ls /some/path/to/file_name

	可以简化为

		fs.cd /some/path
		fs.ls to/file_name
`
}

func (c *commandFsCd) HasTag(CommandTag) bool {
	return false
}

func (c *commandFsCd) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	if handleHelpRequest(c, args, writer) {
		return nil
	}

	path, err := commandEnv.parseUrl(findInputDirectory(args))
	if err != nil {
		return err
	}

	if path == "/" {
		commandEnv.option.Directory = "/"
		return nil
	}

	err = commandEnv.checkDirectory(path)

	if err == nil {
		commandEnv.option.Directory = path
	}

	return err
}
