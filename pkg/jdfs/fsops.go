package jdfs

import (
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/complyue/jdfs/pkg/errors"
	"github.com/complyue/jdfs/pkg/vfs"
	"github.com/golang/glog"
)

type iMeta struct {
	jdfPath string
	name    string

	dev   int64
	inode vfs.InodeID
	attrs vfs.InodeAttributes
}

func (im iMeta) childPath(name string) string {
	if len(im.jdfPath) > 0 && im.jdfPath != "." {
		return fmt.Sprintf("%s/%s", im.jdfPath, name)
	}
	// 1st level child of root dir
	return name
}

func statFileHandle(icfh *icfHandle) (inoM iMeta, err error) {
	var inoFI os.FileInfo
	jdfPath := icfh.f.Name()
	if inoFI, err = icfh.f.Stat(); err != nil {
		glog.Fatalf("stat error through open file handle on [%s]:[%s] - %+v",
			jdfsRootPath, jdfPath, errors.RichError(err))
	}
	if im := fi2im(jdfPath, inoFI); im.inode != icfh.inode {
		glog.Fatalf("opened inode [%d] [%s]:[%s] changed to [%d] ?!",
			icfh.inode, jdfsRootPath, jdfPath, im.inode)
	} else {
		inoM = im
	}
	return
}

func statInode(inode vfs.InodeID, reachedThrough []string) (
	inoM iMeta, outdatedPaths []string, err error) {
	ok := false

	for iPath := len(reachedThrough) - 1; iPath >= 0; //
	outdatedPaths, iPath = append(outdatedPaths, reachedThrough[iPath]), iPath-1 {
		// jdfs process has jdfRootDir as pwd, so just use the relative jdfPath
		jdfPath := reachedThrough[iPath]
		var inoFI os.FileInfo
		if inoFI, err = os.Lstat(jdfPath); err != nil {
			glog.V(1).Infof("UNREACH inode [%d] not at [%s]:[%s] anymore - %+v",
				inode, jdfsRootPath, jdfPath, err)
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
			glog.V(1).Infof("OUTLAW inode [%d] [%s]:[%s] with file mode [%#o] not revealed to jdfc.",
				inode, jdfsRootPath, jdfPath, inoFI.Mode())
			continue
		}

		if im := fi2im(jdfPath, inoFI); im.inode != inode {
			if inode == vfs.RootInodeID && im.inode == jdfRootInode {
				// fake mounted JDFS root inode to be constant 1
				im.inode = vfs.RootInodeID
				inoM = im
				ok = true
			} else {
				glog.V(1).Infof("ICHG [%s]:[%s] is inode [%d] instead of [%d] now.",
					jdfsRootPath, jdfPath, im.inode, inode)
				continue
			}
		} else if im.dev != jdfRootDevice {
			glog.V(1).Infof("OUTLAW inode [%d] [%s]:[%s] not on same local fs, not revealed to jdfc.",
				inode, jdfsRootPath, jdfPath)
			continue
		} else {
			inoM = im
			ok = true

			if glog.V(2) {
				glog.Infof("STAT [%d] [%s]:[%s] nlink=%d, size=%d", im.inode, jdfsRootPath, jdfPath,
					im.attrs.Nlink, im.attrs.Size)
			}
		}

		break // got inoM of same inode
	}

	if !ok {
		err = vfs.ENOENT

		if glog.V(1) {
			glog.V(1).Infof("DISAPPEAR inode [%d] disappeared", inode)
		}
	}
	return
}

func readInodeDir(parentM iMeta) (childMs []iMeta, err error) {
	var (
		parentDir *os.File
		childFIs  []os.FileInfo
		childM    iMeta
	)

	parentPath := parentM.jdfPath
	parentDir, err = os.OpenFile(parentPath, os.O_RDONLY, 0)
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
			glog.V(1).Infof("OUTLAW [%s]:[%s]/[%s] with file mode [%#o] not revealed to jdfc.",
				jdfsRootPath, parentPath, childFI.Name(), childFI.Mode())
			continue
		}

		childM = fi2im(parentM.childPath(childFI.Name()), childFI)
		if glog.V(2) {
			glog.Infof("LS [%s]:[%s]/[%s] is inode [%v]:[%v]", jdfsRootPath, parentPath, childFI.Name(),
				childM.dev, childM.inode)
		}

		if childM.dev != jdfRootDevice {
			if glog.V(1) {
				glog.Infof("OUTLAW [%d] [%s]:[%s]/[%s] not on same local fs, not revealed to jdfc.",
					childM.inode, jdfsRootPath, parentPath, childFI.Name())
			}
			continue
		}

		childMs = append(childMs, childM)
	}

	return
}

func ts2t(ts syscall.Timespec) int64 {
	return int64(int64(ts.Sec)*int64(time.Second) + ts.Nsec)
}
