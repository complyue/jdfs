package jdfs

import (
	"os"
	"syscall"

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
	op.IoSize = uint32(fsStat.Iosize)
	op.Inodes = fsStat.Files
	op.InodesFree = fsStat.Ffree

	return
}

func fi2im(jdfPath string, fi os.FileInfo) iMeta {
	sd, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		panic(errors.Errorf("Incompatible local file: [%s]", fi.Name))
	}
	return iMeta{
		jdfPath: jdfPath, name: fi.Name(),

		dev: int64(sd.Dev), inode: InodeID(sd.Ino),
		attrs: vfs.InodeAttributes{
			Size:   fi.Size(),
			Nlink:  uint32(sd.Nlink),
			Mode:   fi.Mode(),
			Atime:  ts2t(sd.Atimespec),
			Mtime:  ts2t(sd.Mtimespec),
			Ctime:  ts2t(sd.Ctimespec),
			Crtime: ts2t(sd.Birthtimespec),
			Uid:    sd.Uid, Gid: sd.Gid,
		},
	}
}

func chftimes(f *os.File, nsec int64) error {
	t := syscall.NsecToTimeval(nsec)
	return syscall.Futimes(int(f.Fd()), []syscall.Timeval{
		t, t,
	})
}
