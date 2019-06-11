package jdfs

import (
	"os"
	"syscall"
	"time"

	"github.com/complyue/jdfs/pkg/errors"
	"github.com/complyue/jdfs/pkg/vfs"
)

func statFS(rootDir *os.File) (op vfs.StatFSOp, err error) {

	// TODO syscall.Fstatfs is missing as of Go1.12.5,
	//      figure out how to support this

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
			Atime:  ts2t(sd.Atim),
			Mtime:  ts2t(sd.Mtim),
			Ctime:  ts2t(sd.Ctim),
			Crtime: ts2t(sd.Ctim),
		},
	}
}
