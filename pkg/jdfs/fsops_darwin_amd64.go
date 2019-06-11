package jdfs

import (
	"os"
	"syscall"
	"time"

	"github.com/complyue/jdfs/pkg/errors"
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
	op.IoSize = uint32(fsStat.Iosize)
	op.Inodes = fsStat.Files
	op.InodesFree = fsStat.Ffree

	return
}

func ts2t(ts syscall.Timespec) time.Time {
	return time.Unix(ts.Sec, ts.Nsec)
}

func fi2in(fi os.FileInfo) iNode {
	sd, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		panic(errors.Errorf("Incompatible local file: [%s]", fi.Name))
	}
	return iNode{
		dev: int64(sd.Dev), inode: InodeID(sd.Ino),
		attrs: InodeAttributes{
			Nlink:  uint32(sd.Nlink),
			Mode:   os.FileMode(sd.Mode),
			Atime:  ts2t(sd.Atimespec),
			Mtime:  ts2t(sd.Mtimespec),
			Ctime:  ts2t(sd.Ctimespec),
			Crtime: ts2t(sd.Birthtimespec),
		},
	}
}
