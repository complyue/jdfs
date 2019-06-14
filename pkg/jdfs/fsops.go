package jdfs

import (
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/complyue/jdfs/pkg/vfs"
	"github.com/golang/glog"
)

var (
	// absolute path at jdfs host for the mounted root dir
	jdfsRootPath string

	// hold the JDFS mounted root dir open, so as to prevent it from unlinked,
	// until jdfc disconnected.
	jdfRootDir *os.File

	// device of JDFS mounted root dir
	//
	// nested directory with other filesystems mounted will be hidden to jdfc
	jdfRootDevice int64

	// inode value of the JDFS mounted root dir
	//
	// jdfc is not restricted to only mount root of local filesystem at jdfs host,
	// in case a nested dir is mounted as JDFS root, inode of mounted root will be other
	// than 1, which is the constant for FUSE fs root.
	jdfRootInode vfs.InodeID
)

type iMeta struct {
	parentPath string
	name       string

	dev   int64
	inode vfs.InodeID
	attrs vfs.InodeAttributes
}

func (im iMeta) jdfPath() string {
	if len(im.parentPath) > 0 {
		return fmt.Sprintf("%s/%s", im.parentPath, im.name)
	}
	return im.name
}

func statInode(inode vfs.InodeID, reachedThrough []string) (
	inoM iMeta, outdatedPaths []string, err error) {

	for iPath := len(reachedThrough) - 1; iPath >= 0; //
	outdatedPaths, iPath = append(outdatedPaths, reachedThrough[iPath]), iPath-1 {
		// jdfs process has jdfRootDir as pwd, so just use the relative jdfPath
		jdfPath := reachedThrough[iPath]
		var inoFI os.FileInfo
		if inoFI, err = os.Lstat(jdfPath); err != nil {
			glog.V(1).Warningf("jdfs [%s]:[%s] disappeared - %+v", jdfsRootPath, jdfPath, err)
			continue
		}

		if inoFI.IsDir() {
			// a dir
		} else if inoFI.Mode().IsRegular() {
			// a regular file
		} else if (inoFI.Mode() & os.ModeSymlink) != 0 {
			// a symlink
		} else {
			// a file not reigned by JDFS
			glog.V(1).Warningf("jdfs [%s]:[%s] with file mode [%#o] not revealed to jdfc.",
				jdfsRootPath, jdfPath, inoFI.Mode())
			continue
		}

		if im := fi2im("", inoFI); im.inode != inode {
			glog.V(1).Warningf("jdfs [%s]:[%s] is inode [%v] instead of [%v] now.",
				jdfsRootPath, jdfPath, im.inode, inode)
			continue
		} else if im.dev != jdfRootDevice {
			glog.V(1).Warningf("jdfs [%s]:[%s] not on same local fs, not revealed to jdfc.",
				jdfsRootPath, jdfPath)
			continue
		} else {
			inoM = im
		}

		break // got inoM of same inode
	}

	if err == nil && inoM == nil {
		err = vfs.ENOENT
		return
	}

	return
}

func readInodeDir(parentInode vfs.InodeID, reachedThrough []string) (
	parentM iMeta, childMs []iMeta, outdatedPaths []string, err error) {
	var (
		parentDir *os.File
		childFIs  []os.FileInfo
		childM    iMeta
	)

	if parentM, outdatedPaths, err = statInode(
		parentInode, reachedThrough,
	); err != nil {
		return
	}

	parentDir, err = os.OpenFile(parentM.jdfPath(), os.O_RDONLY, 0)
	if err != nil {
		return
	}
	defer parentDir.Close()

	// TODO should either prevent extremely large directories, or implement out-of-core
	//      handling of them, to prefetch huge amount of child inodes in-core may
	//      overload the jdfs host or even crash it.
	if childFIs, err = parentDir.Readdir(0); err != nil {
		return
	}
	if len(childFIs) > 0 {
		childMs = make([]iMeta, 0, len(childFIs))
	}
	for _, childFI := range childFIs {
		if childFI.IsDir() {
			// a dir
		} else if childFI.Mode().IsRegular() {
			// a regular file
		} else if (childFI.Mode() & os.ModeSymlink) != 0 {
			// a symlink
		} else {
			// a file not reigned by JDFS
			glog.V(1).Warningf("jdfs [%s]:[%s]/[%s] with file mode [%#o] not revealed to jdfc.",
				jdfsRootPath, parentPath, childFI.Name(), childFI.Mode())
			continue
		}

		if childM = fi2im(parentPath, childFI); childM.dev != jdfRootDevice {
			glog.V(1).Warningf("jdfs [%s]:[%s]/[%s] not on same local fs, not revealed to jdfc.",
				jdfsRootPath, parentPath, childFI.Name())
			continue
		}

		childMs = append(childMs, childM)
	}

	return
}

func ts2t(ts syscall.Timespec) time.Time {
	return time.Unix(ts.Sec, ts.Nsec)
}
