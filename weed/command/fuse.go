package command

func init() {
	cmdFuse.Run = runFuse // break init cycle
}

var cmdFuse = &Command{
	UsageLine: "fuse /mnt/mount/point -o \"filer=localhost:8888,filer.path=/\"",
	Short:     "允许通过 linux 的 mount 命令使用 weed",
	Long: `允许通过 linux 的 mount 命令使用 weed

  你可以在 mount 命令中使用 -t weed:
  mv weed /sbin/mount.weed
  mount -t weed fuse /mnt -o "filer=localhost:8888,filer.path=/"

  或者你可以在 mount 命令中使用 -t fuse:
  mv weed /sbin/weed
  mount -t fuse.weed fuse /mnt -o "filer=localhost:8888,filer.path=/"
  mount -t fuse "weed#fuse" /mnt -o "filer=localhost:8888,filer.path=/"

  无需修改 /sbin 即可使用:
  mount -t fuse./home/user/bin/weed fuse /mnt -o "filer=localhost:8888,filer.path=/"
  mount -t fuse "/home/user/bin/weed#fuse" /mnt -o "filer=localhost:8888,filer.path=/"

  要传递多个参数请使用引号,例如:
  mount -t weed fuse /mnt -o "filer='192.168.0.1:8888,192.168.0.2:8888',filer.path=/"

  查看有效选项请运行 "weed mount --help"
  `,
}

func GetFuseCommandName() string {
	return cmdFuse.Name()
}
