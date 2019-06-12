package jdfs

import (
	"os"
	"syscall"
	"time"

	"github.com/complyue/jdfs/pkg/errors"
	"github.com/complyue/jdfs/pkg/vfs"
)

func statFS(rootDir *os.File) (op vfs.StatFSOp, err error) {

	var fsStat syscall.Statfs_t
	if err = syscall.Fstatfs(int(rootDir.Fd()), &fsStat); err != nil {
		return
	}

	op.BlockSize = uint32(fsStat.Bsize)
	op.Blocks = fsStat.Blocks
	op.BlocksFree = fsStat.Bfree
	op.BlocksAvailable = fsStat.Bavail
	op.IoSize = uint32(fsStat.Bsize)
	op.Inodes = fsStat.Files
	op.InodesFree = fsStat.Ffree

	return
}

func ts2t(ts syscall.Timespec) time.Time {
	return time.Unix(ts.Sec, ts.Nsec)
}

func fi2im(fi os.FileInfo) iMeta {
	sd, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		panic(errors.Errorf("Incompatible local file: [%s]", fi.Name))
	}
	return iMeta{
		dev: int64(sd.Dev), inode: InodeID(sd.Ino),
		attrs: InodeAttributes{
			Nlink:  uint32(sd.Nlink),
			Mode:   os.FileMode(sd.Mode),
			Atime:  ts2t(sd.Atim),
			Mtime:  ts2t(sd.Mtim),
			Ctime:  ts2t(sd.Ctim),
			Crtime: ts2t(sd.Ctim),
		},
	}
}

func chftimes(f *os.File, nsec int64) error {
	t := syscall.NsecToTimeval(nsec)
	return syscall.Futimes(int(f.Fd()), []syscall.Timeval{
		t, t,
	})
}
