package jdfs

import (
	"os"
	"syscall"

	"github.com/complyue/jdfs/pkg/errors"
	"github.com/complyue/jdfs/pkg/vfs"
)

func statFS(rootDir *os.File) (op vfs.StatFSOp, err error) {

	// TODO syscall.Fstatfs is missing as of Go1.12.5,
	//      figure out how to support this

	return
}

func fi2im(fi os.FileInfo) iMeta {
	sd, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		panic(errors.Errorf("Incompatible local file: [%s]", fi.Name))
	}
	return iMeta{
		parentPath: parentPath, name: fi.Name(),

		dev: int64(sd.Dev), inode: InodeID(sd.Ino),
		attrs: InodeAttributes{
			Nlink:  uint32(sd.Nlink),
			Mode:   os.FileMode(sd.Mode),
			Atime:  ts2t(sd.Atim),
			Mtime:  ts2t(sd.Mtim),
			Ctime:  ts2t(sd.Ctim),
			Crtime: ts2t(sd.Ctim),

			Uid: sd.Uid, Gid: sd.Gid,
		},
	}
}

func chftimes(f *os.File, nsec int64) error {
	t := syscall.NsecToTimeval(nsec)
	return syscall.Futimes(int(f.Fd()), []syscall.Timeval{
		t, t,
	})
}
