package shell

import (
	"fmt"
	"io"
	"strconv"
	"time"
)

func init() {
	Commands = append(Commands, &commandSleep{})
}

// =========== Sleep ==============
type commandSleep struct {
}

func (c *commandSleep) Name() string {
	return "sleep"
}

func (c *commandSleep) Help() string {
	return `睡眠 N 秒(用于模拟长时间运行的任务)

	sleep 5
`
}

func (c *commandSleep) HasTag(CommandTag) bool {
	return false
}

func (c *commandSleep) Do(args []string, _ *CommandEnv, _ io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("sleep requires a seconds argument")
	}
	seconds, err := strconv.Atoi(args[0])
	if err != nil || seconds <= 0 {
		return fmt.Errorf("sleep duration must be a positive integer, got %q", args[0])
	}
	time.Sleep(time.Duration(seconds) * time.Second)
	return nil
}
