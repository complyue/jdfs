package jdfs

import (
	"os"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/complyue/jdfs/pkg/errors"
	"github.com/complyue/jdfs/pkg/vfs"
)

func statFS(rootDir *os.File) (op vfs.StatFSOp, err error) {
	var fsStat unix.Statvfs_t
	if err = unix.Fstatvfs(int(rootDir.Fd()), &fsStat); err != nil {
		return
	}

	op.BlockSize = uint32(fsStat.Frsize)
	op.Blocks = fsStat.Blocks
	op.BlocksFree = fsStat.Bfree
	op.BlocksAvailable = fsStat.Bavail
	op.IoSize = uint32(4096)
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

		dev: int64(sd.Dev), inode: vfs.InodeID(sd.Ino),
		attrs: vfs.InodeAttributes{
			Size:   uint64(fi.Size()),
			Nlink:  uint32(sd.Nlink),
			Mode:   fi.Mode(),
			Atime:  ts2t(sd.Atim),
			Mtime:  ts2t(sd.Mtim),
			Ctime:  ts2t(sd.Ctim),
			Crtime: ts2t(sd.Ctim),
			Uid:    sd.Uid, Gid: sd.Gid,
		},
	}
}

func chftimes(f *os.File, jdfPath string, nsec int64) error {
	t := syscall.Timespec{
		Sec: nsec / 1e9, Nsec: nsec % 1e9,
	}
	return syscall.UtimesNano(jdfPath, []syscall.Timespec{
		t, t,
	})
}

// Solaris seems using file semantics for xattr,
// and Go stdlib has no support for it yet.
//
// TODO add the support when Go does or a proper Go lib found

func fremovexattr(fd int, name string) error {
	return vfs.ENOATTR
}

func removexattr(jdfPath, name string) error {
	return vfs.ENOATTR
}

func fgetxattr(fd int, name string, buf []byte) (int, error) {
	return 0, vfs.ENOATTR
}

func getxattr(jdfPath, name string, buf []byte) (int, error) {
	return 0, vfs.ENOATTR
}

func flistxattr(fd int, buf []byte) (int, error) {
	return 0, nil
}

func listxattr(jdfPath string, buf []byte) (int, error) {
	return 0, nil
}

func fsetxattr(fd int, name string, buf []byte, flags int) error {
	return vfs.ENOSPC
}

func setxattr(jdfPath, name string, buf []byte, flags int) error {
	return vfs.ENOSPC
}
