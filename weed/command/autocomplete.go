package command

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/posener/complete"
	completeinstall "github.com/posener/complete/cmd/install"
	flag "github.com/seaweedfs/seaweedfs/weed/util/fla9"
)

func AutocompleteMain(commands []*Command) bool {
	subCommands := make(map[string]complete.Command)
	helpSubCommands := make(map[string]complete.Command)
	for _, cmd := range commands {
		flags := make(map[string]complete.Predictor)
		cmd.Flag.VisitAll(func(flag *flag.Flag) {
			flags["-"+flag.Name] = complete.PredictAnything
		})

		subCommands[cmd.Name()] = complete.Command{
			Flags: flags,
		}
		helpSubCommands[cmd.Name()] = complete.Command{}
	}
	subCommands["help"] = complete.Command{Sub: helpSubCommands}

	globalFlags := make(map[string]complete.Predictor)
	flag.VisitAll(func(flag *flag.Flag) {
		globalFlags["-"+flag.Name] = complete.PredictAnything
	})

	weedCmd := complete.Command{
		Sub:         subCommands,
		Flags:       globalFlags,
		GlobalFlags: complete.Flags{"-h": complete.PredictNothing},
	}
	cmp := complete.New("weed", weedCmd)

	return cmp.Complete()
}

func printAutocompleteScript(shell string) bool {
	bin, err := os.Executable()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get executable path: %s\n", err)
		return false
	}
	binPath, err := filepath.Abs(bin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get absolute path: %s\n", err)
		return false
	}

	switch shell {
	case "bash":
		fmt.Printf("complete -C %q weed\n", binPath)
	case "zsh":
		fmt.Printf("autoload -U +X bashcompinit && bashcompinit\n")
		fmt.Printf("complete -o nospace -C %q weed\n", binPath)
	case "fish":
		fmt.Printf(`function __complete_weed
    set -lx COMP_LINE (commandline -cp)
    test -z (commandline -ct)
    and set COMP_LINE "$COMP_LINE "
    %q
end
complete -f -c weed -a "(__complete_weed)"
`, binPath)
	default:
		fmt.Fprintf(os.Stderr, "unsupported shell: %s. Supported shells: bash, zsh, fish\n", shell)
		return false
	}
	return true
}

func installAutoCompletion() bool {
	if runtime.GOOS == "windows" {
		fmt.Println("Windows is not supported")
		return false
	}

	err := completeinstall.Install("weed")
	if err != nil {
		fmt.Printf("install failed! %s\n", err)
		return false
	}
	fmt.Printf("autocompletion is enabled. Please restart your shell.\n")
	return true
}

func uninstallAutoCompletion() bool {
	if runtime.GOOS == "windows" {
		fmt.Println("Windows is not supported")
		return false
	}

	err := completeinstall.Uninstall("weed")
	if err != nil {
		fmt.Printf("uninstall failed! %s\n", err)
		return false
	}
	fmt.Printf("autocompletion is disabled. Please restart your shell.\n")
	return true
}

var cmdAutocomplete = &Command{
	Run:       runAutocomplete,
	UsageLine: "autocomplete [shell]",
	Short:     "生成或安装 shell 自动补全脚本",
	Long: `生成 shell 自动补全脚本或将其安装到你的 shell 配置中。

用法:
    weed autocomplete [bash|zsh|fish]  # 将自动补全脚本打印到 stdout
    weed autocomplete install          # 安装到 shell 配置文件

    当提供 shell 名称时,自动补全脚本会打印到 stdout。
    你可以手动将其添加到 shell 配置中,例如:

        # 对于 bash:
        weed autocomplete bash >> ~/.bashrc

        # 或者在你的 shell 配置中使用 eval:
        eval "$(weed autocomplete bash)"

    当提供 'install'(或无参数)时,脚本会自动安装到你的 shell 配置文件中。

    支持的 shell 有 bash、zsh 和 fish。
    不支持 Windows。

`,
}

func runAutocomplete(cmd *Command, args []string) bool {
	if len(args) == 0 {
		// Default behavior: install
		return installAutoCompletion()
	}

	if len(args) > 1 {
		cmd.Usage()
		return false
	}

	shell := args[0]
	if shell == "install" {
		return installAutoCompletion()
	}

	// Print the autocomplete script for the specified shell
	return printAutocompleteScript(shell)
}

var cmdUnautocomplete = &Command{
	Run:       runUnautocomplete,
	UsageLine: "autocomplete.uninstall",
	Short:     "卸载自动补全",
	Long: `在 shell 中卸载 weed 自动补全。

    不支持 Windows。

`,
}

func runUnautocomplete(cmd *Command, args []string) bool {
	if len(args) != 0 {
		cmd.Usage()
	}

	return uninstallAutoCompletion()
}
