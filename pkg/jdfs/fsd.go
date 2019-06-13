package jdfs

import (
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/complyue/jdfs/pkg/errors"
	"github.com/complyue/jdfs/pkg/vfs"
	"github.com/golang/glog"
)

// In-core filesystem data

type (
	InodeID         = vfs.InodeID
	InodeAttributes = vfs.InodeAttributes
)

type iMeta struct {
	dev   int64
	inode InodeID
	attrs InodeAttributes
}

// in-core inode info
type icInode struct {
	// embed an inode meta data struct
	iMeta

	// the in-core record will be freed when reference count is decreased to zero
	refcnt int

	// paths through which this inode has been reached
	reachedThrough []string

	// last time at which attrs/children are refreshed
	lastChecked time.Time

	// cached children of a dir. will always be nil for non-dir inode; and will be nil
	// for a dir after cache is invalidated, if non-nil, the map is per-see at
	// lastChecked time.
	// todo is there needs to preserve directory order? if so an ordered map should be used.
	children map[string]InodeID
}

// in-core handle to a dir held open
type icdHandle struct {
	isi     int
	entries []vfs.DirEnt
}

// in-core handle to a regular file held open
type icfHandle struct {
	isi int
	f   *os.File
}

// in-core filesystem data
//
// a process should have only one icd active,
// with its pwd chdir'ed to the mounted rootDir with icd.init()
type icFSD struct {
	// hold the JDFS mounted root dir open, so as to prevent it from unlinked,
	// until jdfc disconnected.
	rootDir *os.File

	// device of JDFS mount root
	//
	// nested directory with other filesystems mounted will be hidden to jdfc
	rootDevice int64

	// inode value of the JDFS mount root
	//
	// jdfc is not restricted to only mount root of local filesystem of jdfs,
	// in case a nested dir is mounted as JDFS root, inode of mounted root will be other
	// than 1, which is the constant for FUSE fs root.
	rootInode vfs.InodeID

	// registry of in-core info of inodes
	regInodes   map[InodeID]int // map inode ID to indices into stoInodes
	stoInodes   []icInode       // flat storage of icInodes
	freeInoIdxs []int           // free list of indices into stoInodes

	// registry of dir handles held open, a dir handle value is index into this slice
	dirHandles []icdHandle // flat storage of handles
	freeDHIdxs []int       // free list of indices into dirHandles

	// registry of file handles held open, a file handle value is index into this slice
	fileHandles []icfHandle // flat storage of handles
	freeFHIdxs  []int       // free list of indices into fileHandles

	// guard access to session data structs
	mu sync.Mutex
}

func (icd *icFSD) init(rootPath string, readOnly bool) error {
	var flags int
	if readOnly {
		flags = os.O_RDONLY
	} else {
		flags = os.O_RDWR
	}
	rootFI, err := os.Lstat(rootPath)
	if err != nil {
		return errors.Errorf("Bad jdfs path: [%s] - %+v", rootPath, err)
	}
	if !rootFI.IsDir() {
		return errors.Errorf("Not a dir at jdfs: [%s]", rootPath)
	}
	if err = os.Chdir(rootPath); err != nil {
		return errors.Errorf("Error chdir to jdfs path: [%s] - %+v", rootPath, err)
	}
	rootDir, err := os.OpenFile(rootPath, flags, 0)
	if err != nil {
		return errors.Errorf("Error open jdfs path: [%s] - %+v", rootPath, err)
	}

	inode := fi2im(rootFI)

	icd.mu.Lock()
	defer icd.mu.Unlock()

	icd.rootDir = rootDir
	icd.rootDevice = inode.dev
	icd.rootInode = inode.inode

	// todo sophisticate initial in-core data allocation,
	// may base on statistics from local fs and config.
	icd.regInodes = make(map[InodeID]int)
	icd.stoInodes = nil
	icd.freeInoIdxs = nil
	icd.fileHandles = []icfHandle{icfHandle{}} // reserve 0 for nil handle
	icd.freeFHIdxs = nil

	isi := icd.loadInode(rootFI, "/")
	if isi != 0 {
		panic("root inode got isi other than zero ?!?")
	}
	ici := &icd.stoInodes[isi]
	ici.refcnt++ // not really needed as root inode won't be forgotten anyway

	return nil
}

// must have icd.mu locked
func (icd *icFSD) loadInode(fi os.FileInfo, jdfPath string) (isi int) {
	inode := fi2im(fi)

	if inode.dev != icd.rootDevice {
		glog.Warningf("Nested mount point [%s] under [%s] not supported by JDFS.",
			jdfPath, icd.rootDir.Name())
		return -1
	}

	var ok bool
	isi, ok = icd.regInodes[inode.inode]
	if ok {
		// hard link to a known inode
		ici := &icd.stoInodes[isi]
		if inode.inode != ici.inode {
			panic(errors.New("regInodes corrupted ?!"))
		}
		if inode.dev != ici.dev {
			panic(errors.New("inode device changed ?!"))
		}
		for _, reachPath := range ici.reachedThrough {
			if reachPath == jdfPath {
				panic(errors.New("in-core inode reached by same path twice ?!"))
			}
		}

		// reached from a new path
		ici.reachedThrough = append(ici.reachedThrough, jdfPath)

		// update meta attrs
		ici.attrs = inode.attrs
		ici.lastChecked = time.Now()

		// invalidate cached dir children
		// (may actually be not needed as dirs are not allowed to be hard linked)
		ici.children = nil
		return
	} else {
		// 1st time reaching an inode
		if nfi := len(icd.freeInoIdxs); nfi > 0 {
			isi = icd.freeInoIdxs[nfi-1]
			icd.freeInoIdxs = icd.freeInoIdxs[:nfi-1]
		} else {
			isi = len(icd.stoInodes)
			icd.stoInodes = append(icd.stoInodes, icInode{})
		}
		ici := &icd.stoInodes[isi]
		*ici = icInode{
			iMeta: inode,

			// not necessarily referenced by fuse kernel,
			// will be increased by respective ops
			refcnt: 0,
			// TODO this jdfs implementation prefetches children inodes aggressively,
			//      impacts of extreme huge directories is yet to be addressed.

			reachedThrough: []string{jdfPath},
			lastChecked:    time.Now(),
			children:       nil,
		}
		return
	}
	panic("should never reach here")
}

func (icd *icFSD) ForgetInode(inode InodeID, n int) {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	if inode == icd.rootInode {
		panic(errors.Errorf("forget root ?!"))
	}

	if n <= 0 {
		panic(errors.Errorf("forget %d ref ?!", n))
	}

	isi, ok := icd.regInodes[inode]
	if !ok {
		panic(errors.Errorf("inode [%v] not in-core ?!", inode))
	}
	ici := &icd.stoInodes[isi]

	ici.refcnt -= n

	if ici.refcnt < 0 {
		panic(errors.Errorf("fuse ref counting problem ?!"))
	}

	if ici.refcnt > 0 {
		return // still referenced
	}

	delete(icd.regInodes, inode)
	icd.stoInodes[isi] = icInode{} // clear all fields to zero values
	icd.freeInoIdxs = append(icd.freeInoIdxs, isi)
}

// must have icd.mu locked
func (icd *icFSD) getInode(inode InodeID) *icInode {
	isi, ok := icd.regInodes[inode]
	if !ok {
		glog.Errorf("inode not in-core [%v] ?!", inode)
		return nil
	}
	ici := &icd.stoInodes[isi]
	if ici.inode != inode {
		glog.Errorf("inode disappeared [%v] ?!", inode)
		return nil
	}

	return ici
}

// must have icd.mu locked
func (ici *icInode) refreshInode(icd *icFSD, forWrite bool,
	withF func(path string, f *os.File, fi os.FileInfo) (keepF bool, err error),
) (err error) {
	openFlags := os.O_RDONLY
	if forWrite {
		openFlags = os.O_RDWR
	}
	var (
		inoPath string
		inoF    *os.File
		keepF   = false
	)
	defer func() {
		if inoF != nil && (err != nil || !keepF) {
			inoF.Close()
		}
	}()
	var inoFI os.FileInfo
	var im iMeta
	for iPath := len(ici.reachedThrough) - 1; iPath >= 0; ici.reachedThrough, iPath = ici.reachedThrough[:iPath], iPath-1 {
		inoPath = ici.reachedThrough[iPath]
		// jdfs process has mounted root dir as pwd, so can just open jdfPath
		if inoF != nil {
			inoF.Close()
			inoF = nil
		}
		if inoFI, err = os.Lstat(inoPath); err != nil {
			glog.Warningf("JDFS [%s]:[%s] disappeared - %+v", icd.rootDir.Name(), inoPath, err)
			continue
		} else if (inoFI.Mode() & os.ModeSymlink) != 0 {
			// reload a symlink
		} else if !inoFI.Mode().IsRegular() {
			// TODO handle other non-regular file cases
			panic(errors.Errorf("unexpected file mode [%v] of [%s]:[%s]", inoFI.Mode(), icd.rootDir.Name(), inoPath))
		}

		if im = fi2im(inoFI); im.inode != ici.inode {
			glog.Warningf("JDFS [%s]:[%s] is inode [%v] instead of [%v] now.",
				icd.rootDir.Name(), inoPath, im.inode, ici.inode)
			continue
		}

		inoF, err = os.OpenFile(inoPath, openFlags, 0)
		if err != nil {
			glog.Warningf("JDFS [%s]:[%s] no longer be inode [%v] - %+v",
				icd.rootDir.Name(), inoPath, ici.inode, err)
			inoF = nil
			continue
		}
		break // got inoF of same inode
	}

	if inoF == nil {
		return vfs.ENOENT
	}

	ici.attrs = im.attrs
	ici.lastChecked = time.Now()

	if withF != nil {
		keepF, err = withF(inoPath, inoF, inoFI)
		if err != nil {
			return
		}
	}

	return
}

// must have icd.mu locked
func (ici *icInode) refreshChildren(icd *icFSD, forWrite bool,
	withParent func(parentPath string, parentDir *os.File, parentFI os.FileInfo) (keepParentF bool, err error),
	withChild func(childName string, cisi int)) (err error) {
	return ici.refreshInode(icd, forWrite,
		func(parentPath string, parentDir *os.File, parentFI os.FileInfo) (keepF bool, err error) {
			ici.children = nil
			if parentDir == nil || !parentFI.IsDir() { // not a dir anymore
				return
			}

			if withParent != nil {
				keepF, err = withParent(parentPath, parentDir, parentFI)
				if err != nil {
					return
				}
			}

			// TODO should either prevent extremely large directories, or implement out-of-core handling of them,
			//      to prefetch huge amount of child inodes in-core may overload the jdfs host or even crash it.
			var cFIs []os.FileInfo
			cFIs, err = parentDir.Readdir(0)
			if err != nil {
				return
			}
			ici.children = make(map[string]InodeID, len(cFIs))
			for _, cfi := range cFIs {
				if cfi.IsDir() {
					// a dir
				} else if cfi.Mode().IsRegular() {
					// a regular file
				} else {
					// hide non-regular files to jdfc
					continue
				}

				cisi := icd.loadInode(cfi, fmt.Sprintf("%s/%s", parentPath, cfi.Name()))
				if cisi < 0 {
					// most prolly a nested mount point, invisible to jdfc
					continue
				}
				cici := &icd.stoInodes[cisi]
				ici.children[cfi.Name()] = cici.inode

				if withChild != nil {
					withChild(cfi.Name(), cisi)
				}
			}

			return
		})
}

func (icd *icFSD) GetInode(inode InodeID) *icInode {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	return icd.getInode(inode)
}

func (icd *icFSD) FetchInode(inode InodeID) *vfs.InodeAttributes {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	ici := icd.getInode(inode)
	if ici == nil {
		return nil
	}

	if time.Now().Sub(ici.lastChecked) > vfs.META_ATTRS_CACHE_TIME {
		if err := ici.refreshInode(icd, false, nil); err != nil {
			panic(errors.Errorf("inode [%v] lost - %+v", ici.inode, err))
		}
	}

	return &ici.attrs
}

func (icd *icFSD) LookUpInode(parent InodeID, name string) (*vfs.ChildInodeEntry, error) {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	isi, ok := icd.regInodes[parent]
	if !ok {
		panic(errors.Errorf("parent inode [%v] not in-core ?!", parent))
	}
	ici := &icd.stoInodes[isi]

	var matchedChild *icInode
	if ici.children == nil || time.Now().Sub(ici.lastChecked) > vfs.DIR_CHILDREN_CACHE_TIME {
		// reload children
		if err := ici.refreshChildren(icd, false, nil, func(childName string, cisi int) {
			if childName == name {
				matchedChild = &icd.stoInodes[cisi]
				matchedChild.refcnt++
			}
		}); err != nil {
			// parent dir went away, this possible ?
			return nil, err
		}
	} else if cInode, ok := ici.children[name]; ok {
		matchedChild = icd.getInode(cInode)
	}

	if matchedChild == nil {
		return nil, vfs.ENOENT
	}
	return &vfs.ChildInodeEntry{
		Child:                matchedChild.iMeta.inode,
		Generation:           0,
		Attributes:           matchedChild.iMeta.attrs,
		AttributesExpiration: time.Now().Add(vfs.META_ATTRS_CACHE_TIME),
		EntryExpiration:      time.Now().Add(vfs.DIR_CHILDREN_CACHE_TIME),
	}, nil
}

func (icd *icFSD) SetInodeAttributes(inode InodeID,
	chgSize, chgMode, chgMtime bool,
	sz uint64, mode uint32, mNsec int64,
) *icInode {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	ici := icd.getInode(inode)
	if ici == nil {
		return nil
	}
	if err := ici.refreshInode(icd, true, func(inoPath string, inoF *os.File, inoFI os.FileInfo) (keepF bool, err error) {

		if chgSize {
			if err = inoF.Truncate(int64(sz)); err != nil {
				return
			}
		}

		if chgMode {
			if err = inoF.Chmod(os.FileMode(mode)); err != nil {
				return
			}
		}

		if chgMtime {
			if err = chftimes(inoF, mNsec); err != nil {
				return
			}
		}

		// stat local fs again for new meta attrs
		inoFI, err = os.Lstat(inoPath)
		if err != nil {
			return
		}
		im := fi2im(inoFI)
		if im.inode != ici.inode {
			panic("inode changed ?!")
		}
		ici.attrs = im.attrs
		ici.lastChecked = time.Now()

		return
	}); err != nil {
		panic(errors.Errorf("failed updating inode [%v] - %+v", ici.inode, err))
	}

	return ici
}

func (icd *icFSD) MkDir(parent InodeID, name string, mode uint32) (ce *vfs.ChildInodeEntry, err error) {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	isi, ok := icd.regInodes[parent]
	if !ok {
		panic(errors.Errorf("parent inode [%v] not in-core ?!", parent))
	}
	ici := &icd.stoInodes[isi]

	if err = ici.refreshChildren(icd, true,
		func(parentPath string, parentDir *os.File, parentFI os.FileInfo) (keepF bool, err error) {
			if err = os.Mkdir(fmt.Sprintf("%s/%s", parentPath, name), os.FileMode(mode)); err != nil {
				return
			}
			return
		}, func(childName string, cisi int) {
			if childName == name {
				cici := &icd.stoInodes[cisi]
				cici.refcnt++
				ce = &vfs.ChildInodeEntry{
					Child:                cici.iMeta.inode,
					Generation:           0,
					Attributes:           cici.iMeta.attrs,
					AttributesExpiration: time.Now().Add(vfs.META_ATTRS_CACHE_TIME),
					EntryExpiration:      time.Now().Add(vfs.DIR_CHILDREN_CACHE_TIME),
				}
			}
		}); err != nil {
		glog.Warningf("inode lost [%v] - %+v", ici.inode, err)
		return
	}

	return
}

func (icd *icFSD) CreateFile(parent InodeID, name string, mode uint32) (
	ce *vfs.ChildInodeEntry, handle vfs.HandleID, err error) {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	isi, ok := icd.regInodes[parent]
	if !ok {
		panic(errors.Errorf("parent inode [%v] not in-core ?!", parent))
	}
	ici := &icd.stoInodes[isi]

	if err = ici.refreshInode(icd, true,
		func(parentPath string, parentDir *os.File, parentFI os.FileInfo) (keepF bool, err error) {
			var (
				childPath = fmt.Sprintf("%s/%s", parentPath, name)
				cF        *os.File
				cisi      int
				cici      *icInode
			)
			cF, err = os.OpenFile(
				childPath,
				os.O_CREATE|os.O_EXCL, os.FileMode(mode),
			)
			if err != nil {
				return
			}
			var cFI os.FileInfo
			if cFI, err = cF.Stat(); err != nil {
				return
			}
			cisi = icd.loadInode(cFI, childPath)
			cici = &icd.stoInodes[cisi]
			cici.refcnt++

			if ici.children != nil {
				ici.children[cFI.Name()] = cici.inode
			}

			var hsi int
			if nFreeHdls := len(icd.freeFHIdxs); nFreeHdls > 0 {
				hsi = icd.freeFHIdxs[nFreeHdls-1]
				icd.freeFHIdxs = icd.freeFHIdxs[:nFreeHdls-1]
				icd.fileHandles[hsi] = icfHandle{
					isi: cisi, f: cF,
				}
			} else {
				hsi = len(icd.fileHandles)
				icd.fileHandles = append(icd.fileHandles, icfHandle{
					isi: cisi, f: cF,
				})
			}
			handle = vfs.HandleID(hsi)

			ce = &vfs.ChildInodeEntry{
				Child:                cici.inode,
				Generation:           0,
				Attributes:           cici.attrs,
				AttributesExpiration: time.Now().Add(vfs.META_ATTRS_CACHE_TIME),
				EntryExpiration:      time.Now().Add(vfs.DIR_CHILDREN_CACHE_TIME),
			}

			return
		}); err != nil {
		glog.Warningf("inode lost [%v] - %+v", ici.inode, err)
		return
	}

	return
}

func (icd *icFSD) CreateSymlink(parent InodeID, name string, target string) (ce *vfs.ChildInodeEntry, err error) {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	isi, ok := icd.regInodes[parent]
	if !ok {
		panic(errors.Errorf("parent inode [%v] not in-core ?!", parent))
	}
	ici := &icd.stoInodes[isi]

	if err = ici.refreshChildren(icd, true,
		func(parentPath string, parentDir *os.File, parentFI os.FileInfo) (keepF bool, err error) {
			if err = os.Symlink(target, fmt.Sprintf("%s/%s", parentPath, name)); err != nil {
				return
			}
			return
		}, func(childName string, cisi int) {
			if childName == name {
				cici := &icd.stoInodes[cisi]
				cici.refcnt++
				ce = &vfs.ChildInodeEntry{
					Child:                cici.iMeta.inode,
					Generation:           0,
					Attributes:           cici.iMeta.attrs,
					AttributesExpiration: time.Now().Add(vfs.META_ATTRS_CACHE_TIME),
					EntryExpiration:      time.Now().Add(vfs.DIR_CHILDREN_CACHE_TIME),
				}
			}
		}); err != nil {
		glog.Warningf("inode lost [%v] - %+v", ici.inode, err)
		return
	}

	return
}

func (icd *icFSD) CreateLink(parent InodeID, name string, target InodeID) (ce *vfs.ChildInodeEntry, err error) {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	isi, ok := icd.regInodes[parent]
	if !ok {
		panic(errors.Errorf("parent inode [%v] not in-core ?!", parent))
	}
	ici := &icd.stoInodes[isi]

	isiTarget, ok := icd.regInodes[target]
	if !ok {
		panic(errors.Errorf("target inode [%v] not in-core ?!", target))
	}
	iciTarget := &icd.stoInodes[isiTarget]

	if err = iciTarget.refreshInode(icd, false,
		func(targetPath string, targetDir *os.File, targetFI os.FileInfo) (keepF bool, err error) {
			if err = ici.refreshChildren(icd, true,
				func(parentPath string, parentDir *os.File, parentFI os.FileInfo) (keepF bool, err error) {
					if err = os.Link(targetPath, fmt.Sprintf("%s/%s", parentPath, name)); err != nil {
						return
					}
					return
				}, func(childName string, cisi int) {
					if childName == name {
						cici := &icd.stoInodes[cisi]
						cici.refcnt++
						ce = &vfs.ChildInodeEntry{
							Child:                cici.iMeta.inode,
							Generation:           0,
							Attributes:           cici.iMeta.attrs,
							AttributesExpiration: time.Now().Add(vfs.META_ATTRS_CACHE_TIME),
							EntryExpiration:      time.Now().Add(vfs.DIR_CHILDREN_CACHE_TIME),
						}
					}
				}); err != nil {
				glog.Warningf("inode lost [%v] - %+v", ici.inode, err)
				return
			}
			return
		}); err != nil {
		glog.Warningf("inode lost [%v] - %+v", iciTarget.inode, err)
		return nil, err
	}

	return
}

func (icd *icFSD) Rename(oldParent InodeID, oldName string, newParent InodeID, newName string) (err error) {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	isiOldDir, ok := icd.regInodes[oldParent]
	if !ok {
		panic(errors.Errorf("old parent inode [%v] not in-core ?!", oldParent))
	}
	iciOldDir := &icd.stoInodes[isiOldDir]

	isiNewDir, ok := icd.regInodes[newParent]
	if !ok {
		panic(errors.Errorf("new parent inode [%v] not in-core ?!", newParent))
	}
	iciNewDir := &icd.stoInodes[isiNewDir]

	if err = iciOldDir.refreshInode(icd, true,
		func(oldPath string, oldDir *os.File, oldFI os.FileInfo) (keepF bool, err error) {
			if err = iciNewDir.refreshInode(icd, true,
				func(newPath string, newDir *os.File, newFI os.FileInfo) (keepF bool, err error) {

					if err = os.Rename(
						fmt.Sprintf("%s/%s", oldPath, oldName),
						fmt.Sprintf("%s/%s", newPath, newName),
					); err != nil {
						return
					}

					return
				}); err != nil {
				glog.Warningf("new inode lost [%v] - %+v", iciNewDir.inode, err)
				return
			}
			return
		}); err != nil {
		glog.Warningf("old inode lost [%v] - %+v", iciOldDir.inode, err)
		return
	}

	return
}

func (icd *icFSD) RmDir(parent InodeID, name string) (err error) {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	isi, ok := icd.regInodes[parent]
	if !ok {
		panic(errors.Errorf("parent inode [%v] not in-core ?!", parent))
	}
	ici := &icd.stoInodes[isi]

	if err = ici.refreshInode(icd, true,
		func(parentPath string, parentDir *os.File, parentFI os.FileInfo) (keepF bool, err error) {

			if err = syscall.Rmdir(fmt.Sprintf("%s/%s", parentPath, name)); err != nil {
				return
			}

			ici.children = nil // invalidate cached children

			return
		}); err != nil {
		glog.Warningf("inode lost [%v] - %+v", ici.inode, err)
		return
	}

	return
}

func (icd *icFSD) Unlink(parent InodeID, name string) (err error) {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	isi, ok := icd.regInodes[parent]
	if !ok {
		panic(errors.Errorf("parent inode [%v] not in-core ?!", parent))
	}
	ici := &icd.stoInodes[isi]

	if err = ici.refreshInode(icd, true,
		func(parentPath string, parentDir *os.File, parentFI os.FileInfo) (keepF bool, err error) {

			if err = syscall.Unlink(fmt.Sprintf("%s/%s", parentPath, name)); err != nil {
				return
			}

			ici.children = nil // invalidate cached children

			return
		}); err != nil {
		glog.Warningf("inode lost [%v] - %+v", ici.inode, err)
		return
	}

	return
}

func (icd *icFSD) OpenDir(inode InodeID) (handle vfs.HandleID, err error) {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	isi, ok := icd.regInodes[inode]
	if !ok {
		panic(errors.Errorf("inode [%v] not in-core ?!", inode))
	}
	ici := &icd.stoInodes[isi]

	var entries []vfs.DirEnt
	if len(ici.children) > 0 { // use previous size of children for cap hint
		entries = make([]vfs.DirEnt, 0, len(ici.children))
	}

	if err = ici.refreshChildren(icd, false, nil, func(childName string, cisi int) {
		cici := &icd.stoInodes[cisi]
		entType := vfs.DT_Unknown
		if cici.attrs.Mode.IsDir() {
			entType = vfs.DT_Directory
		} else if cici.attrs.Mode.IsRegular() {
			entType = vfs.DT_FIFO
		} else if cici.attrs.Mode&os.ModeSymlink != 0 {
			entType = vfs.DT_Link
		} else {
			return // hide this strange inode to jdfc
		}
		entries = append(entries, vfs.DirEnt{
			Offset: vfs.DirOffset(len(entries)),
			Inode:  cici.inode, Name: childName,
			Type: entType,
		})
	}); err != nil {
		glog.Warningf("inode lost [%v] - %+v", ici.inode, err)
		return
	}

	var hsi int
	if nFreeHdls := len(icd.freeDHIdxs); nFreeHdls > 0 {
		hsi = icd.freeDHIdxs[nFreeHdls-1]
		icd.freeDHIdxs = icd.freeDHIdxs[:nFreeHdls-1]
		icd.dirHandles[hsi] = icdHandle{
			isi: isi, entries: entries,
		}
	} else {
		hsi = len(icd.dirHandles)
		icd.dirHandles = append(icd.dirHandles, icdHandle{
			isi: isi, entries: entries,
		})
	}
	handle = vfs.HandleID(hsi)

	return
}
