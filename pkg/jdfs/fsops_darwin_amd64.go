package jdfs

import (
	"os"
	"syscall"

	"github.com/complyue/jdfs/pkg/fuse"
)

func statFS(rootDir *os.File) (op fuse.StatFSOp, err error) {

	var fsStat syscall.Statfs_t
	if err = syscall.Fstatfs(int(rootDir.Fd()), &fsStat); err != nil {
		return
	}

	op.BlockSize = uint32(fsStat.Bsize)
	op.Blocks = fsStat.Blocks
	op.BlocksFree = fsStat.Bfree
	op.BlocksAvailable = fsStat.Bavail
	op.IoSize = fsStat.Iosize
	op.Inodes = fsStat.Files
	op.InodesFree = fsStat.Ffree

	return
}
