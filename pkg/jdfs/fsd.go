package jdfs

import (
	"fmt"
	"os"
	"strings"
	"sync"
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
	// for a dir after cache is invalidated, if non-nil, the list is per-see at
	// lastChecked time.
	children []InodeID
}

// in-core handle to an inode held open
type icHandle struct {
	ino InodeID
	f   *os.File
}

// in-core filesystem data
type icFSD struct {
	// hold the JDFS mounted root dir open, so as to prevent it from unlinked,
	// until JDFS client disconnected.
	rootDir *os.File

	// device of JDFS mount root
	//
	// nested directory with other filesystems mounted will be hidden to JDFS client
	rootDevice int64

	// inode value of the JDFS mount root
	//
	// JDFS client is not restricted to only mount root of local filesystem of JDFS server,
	// in case a nested dir is mounted as JDFS root, inode of mounted root will be other
	// than 1, which is the constant for FUSE fs root.
	rootInode vfs.InodeID

	// registry of in-core info of inodes
	regInode    map[InodeID]int // map to index into stoInodes
	stoInodes   []icInode       // flat storage of icInodes
	freeInoIdxs []int           // free list of indices into stoInodes

	// registry of handles held open, a handle value is index into openHandles
	stoHandles  []icHandle // flat storage of handles
	freeHdlIdxs []int      // free list of indices into stoHandles

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
	rootDir, err := os.OpenFile(rootPath, flags, 0)
	if err != nil {
		return errors.Errorf("Bad JDFS server path: [%s]", rootPath)
	}
	// TODO use .Stat() means following symlinks, is this okay ?
	if fi, err := rootDir.Stat(); err != nil || !fi.IsDir() {
		return errors.Errorf("Invalid JDFS server path: [%s]", rootPath)
	} else {

		inode := fi2im(fi)

		icd.mu.Lock()
		defer icd.mu.Unlock()

		icd.rootDir = rootDir
		icd.rootDevice = inode.dev
		icd.rootInode = inode.inode

		// todo sophisticate initial in-core data allocation,
		// may base on statistics from local fs and config.
		icd.regInode = make(map[InodeID]int)
		icd.stoInodes = nil
		icd.freeInoIdxs = nil
		icd.stoHandles = []icHandle{icHandle{}} // reserve 0 for nil handle
		icd.freeHdlIdxs = nil

		ici := icd.loadInode(fi, "/")

		if ici == nil {
			panic(errors.New("root inode not loaded ?!"))
		}

	}
	return nil
}

// must have icd.mu locked
func (icd *icFSD) loadInode(fi os.FileInfo, jdfPath string) (ici *icInode) {
	inode := fi2im(fi)

	if inode.dev != icd.rootDevice {
		glog.Warningf("Nested mount point [%s] under [%s] not supported by JDFS.",
			jdfPath, icd.rootDir.Name())
		return nil
	}

	isi, ok := icd.regInode[inode.inode]
	if ok {
		// hard link to a known inode
		ici = &icd.stoInodes[isi]
		if inode.inode != ici.inode {
			panic(errors.New("regInode corrupted ?!"))
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
		ici.refcnt++
		ici.reachedThrough = append(ici.reachedThrough, jdfPath)

		// update meta attrs
		ici.attrs = inode.attrs
		ici.lastChecked = time.Now()
		ici.children = nil // invalidate cached children list
	} else {
		// 1st time reaching an inode
		if nfi := len(icd.freeInoIdxs); nfi > 0 {
			isi = icd.freeInoIdxs[nfi-1]
			icd.freeInoIdxs = icd.freeInoIdxs[:nfi-1]
		} else {
			isi = len(icd.stoInodes)
			icd.stoInodes = append(icd.stoInodes, icInode{})
		}
		ici = &icd.stoInodes[isi]
		*ici = icInode{
			iMeta: inode,

			refcnt:         1,
			reachedThrough: []string{jdfPath},
			lastChecked:    time.Now(),
			children:       nil,
		}
		return ici
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

	isi, ok := icd.regInode[inode]
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

	delete(icd.regInode, inode)
	icd.stoInodes[isi] = icInode{} // clear all fields to zero values
	icd.freeInoIdxs = append(icd.freeInoIdxs, isi)
}

// must have icd.mu locked
func (icd *icFSD) getInode(inode InodeID) *icInode {
	isi, ok := icd.regInode[inode]
	if !ok {
		panic(errors.Errorf("inode [%v] not in-core ?!", inode))
	}
	ici := &icd.stoInodes[isi]

	return ici
}

// must have icd.mu locked
func (ici *icInode) reloadInode(icd *icFSD, forWrite bool, withFile func(path string, f *os.File, fi os.FileInfo)) bool {
	openFlags := os.O_RDONLY
	if forWrite {
		openFlags = os.O_RDWR
	}
	var err error
	var inoPath string
	var inoF *os.File
	defer func() {
		if inoF != nil {
			inoF.Close()
		}
	}()
	var inoFI os.FileInfo
	var im iMeta
	for iPath := len(ici.reachedThrough) - 1; iPath >= 0; ici.reachedThrough, iPath = ici.reachedThrough[:iPath], iPath-1 {
		inoPath = ici.reachedThrough[iPath]
		// JDFS server process has mounted root dir as pwd, so can just open jdfPath
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
		return false
	}

	ici.attrs = im.attrs
	ici.lastChecked = time.Now()

	if withFile != nil {
		withFile(inoPath, inoF, inoFI)
	}

	return true
}

// must have icd.mu locked
func (ici *icInode) refreshChildren(icd *icFSD, lookUpName string) (reloaded bool, matchedChild *icInode) {
	reloaded = ici.reloadInode(icd, false, func(parentPath string, parentDir *os.File, parentFI os.FileInfo) {

		if parentDir == nil || !parentFI.IsDir() {
			// not a dir anymore
			ici.children = nil
			return
		}
		if ici.children != nil { // clear content, keep capacity
			ici.children = ici.children[:0]
		}
		cFIs, err := parentDir.Readdir(0)
		if err != nil {
			panic(err)
		} else {
			if cap(ici.children) < len(cFIs) {
				ici.children = make([]InodeID, 0, len(cFIs))
			}
			for _, cfi := range cFIs {

				cici := icd.loadInode(cfi, fmt.Sprintf("%s/%s", parentPath, cfi.Name()))

				if cici == nil { // most prolly a nested mount point
					// keep it invisible to JDFS client
					continue
				}

				if cfi.Name() == lookUpName {
					matchedChild = cici
				}

				ici.children = append(ici.children, cici.inode)
			}
		}

	})
	return
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

	if time.Now().Sub(ici.lastChecked) > vfs.META_ATTRS_CACHE_TIME {
		if !ici.reloadInode(icd, false, nil) {
			panic(errors.Errorf("inode [%v] lost", ici.inode))
		}
	}

	return &ici.attrs
}

func (icd *icFSD) LookUpInode(parent InodeID, name string) (*vfs.ChildInodeEntry, error) {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	isi, ok := icd.regInode[parent]
	if !ok {
		panic(errors.Errorf("parent inode [%v] not in-core ?!", parent))
	}
	ici := &icd.stoInodes[isi]

	var reloaded bool
	var matchedChild *icInode
	if ici.children == nil || time.Now().Sub(ici.lastChecked) > vfs.DIR_CHILDREN_CACHE_TIME {
		// reload children
		reloaded, matchedChild = ici.refreshChildren(icd, name)
		if !reloaded {
			return nil, vfs.ENOENT
		}
	} else {
		for _, cInode := range ici.children {
			cisi := icd.regInode[cInode]
			cici := &icd.stoInodes[cisi]
			for _, jdfPath := range cici.reachedThrough {
				if ep := strings.LastIndexByte(jdfPath, '/'); jdfPath[ep+1:] == name {
					matchedChild = cici
				}
			}
		}
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
	if !ici.reloadInode(icd, true, func(inoPath string, inoF *os.File, inoFI os.FileInfo) {

		if chgSize {
			if err := inoF.Truncate(int64(sz)); err != nil {
				panic(err)
			}
		}

		if chgMode {
			if err := inoF.Chmod(os.FileMode(mode)); err != nil {
				panic(err)
			}
		}

		if chgMtime {
			if err := chftimes(inoF, mNsec); err != nil {
				panic(err)
			}
		}

		// stat local fs for new meta attrs
		inoFI, err := os.Lstat(inoPath)
		if err != nil {
			panic(err)
		}
		im := fi2im(inoFI)
		if im.inode != ici.inode {
			panic("inode changed ?!")
		}
		ici.attrs = im.attrs
		ici.lastChecked = time.Now()

	}) {
		panic(errors.Errorf("inode [%v] lost", ici.inode))
	}

	return ici
}

func (icd *icFSD) MkDir(parent InodeID, name string, mode uint32) (ce *vfs.ChildInodeEntry, fsErr error) {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	isi, ok := icd.regInode[parent]
	if !ok {
		panic(errors.Errorf("parent inode [%v] not in-core ?!", parent))
	}
	ici := &icd.stoInodes[isi]

	if !ici.reloadInode(icd, true, func(parentPath string, parentDir *os.File, parentFI os.FileInfo) {

		if fsErr = os.Mkdir(fmt.Sprintf("%s/%s", parentPath, name), os.FileMode(mode)); fsErr != nil {
			return
		}

		if ici.children != nil { // clear content, keep capacity
			ici.children = ici.children[:0]
		}
		cFIs, err := parentDir.Readdir(0)
		if err != nil {
			panic(err)
		} else {
			if cap(ici.children) < len(cFIs) {
				ici.children = make([]InodeID, 0, len(cFIs))
			}
			for _, cfi := range cFIs {

				cici := icd.loadInode(cfi, fmt.Sprintf("%s/%s", parentPath, cfi.Name()))

				if cici == nil { // most prolly a nested mount point
					// keep it invisible to JDFS client
					continue
				}

				if cfi.Name() == name {
					ce = &vfs.ChildInodeEntry{
						Child:                cici.iMeta.inode,
						Generation:           0,
						Attributes:           cici.iMeta.attrs,
						AttributesExpiration: time.Now().Add(vfs.META_ATTRS_CACHE_TIME),
						EntryExpiration:      time.Now().Add(vfs.DIR_CHILDREN_CACHE_TIME),
					}
				}

				ici.children = append(ici.children, cici.inode)
			}
		}

	}) {
		glog.Warningf("inode [%v] lost", ici.inode)
		fsErr = vfs.ENOENT
	}

	return
}

func (icd *icFSD) CreateFile(parent InodeID, name string, mode uint32) (
	ce *vfs.ChildInodeEntry, handle vfs.HandleID, fsErr error) {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	isi, ok := icd.regInode[parent]
	if !ok {
		panic(errors.Errorf("parent inode [%v] not in-core ?!", parent))
	}
	ici := &icd.stoInodes[isi]

	if !ici.reloadInode(icd, true, func(parentPath string, parentDir *os.File, parentFI os.FileInfo) {

		childPath := fmt.Sprintf("%s/%s", parentPath, name)
		var cF *os.File
		var cici *icInode
		cF, fsErr = os.OpenFile(
			childPath,
			os.O_CREATE|os.O_EXCL, os.FileMode(mode),
		)
		if fsErr != nil {
			return
		}
		if cFI, fsErr := cF.Stat(); fsErr != nil {
			return
		} else {
			cici = icd.loadInode(cFI, childPath)
		}

		ici.children = append(ici.children, cici.inode)

		var hsi int
		if nFreeHdls := len(icd.freeHdlIdxs); nFreeHdls > 0 {
			hsi = icd.freeHdlIdxs[nFreeHdls-1]
			icd.freeHdlIdxs = icd.freeHdlIdxs[:nFreeHdls-1]
			icd.stoHandles[hsi] = icHandle{
				ino: cici.inode, f: cF,
			}
		} else {
			hsi = len(icd.stoHandles)
			icd.stoHandles = append(icd.stoHandles, icHandle{
				ino: cici.inode, f: cF,
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

	}) {
		glog.Warningf("inode [%v] lost", ici.inode)
		fsErr = vfs.ENOENT
	}

	return
}

func (icd *icFSD) CreateSymlink(parent InodeID, name string, target string) (ce *vfs.ChildInodeEntry, fsErr error) {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	isi, ok := icd.regInode[parent]
	if !ok {
		panic(errors.Errorf("parent inode [%v] not in-core ?!", parent))
	}
	ici := &icd.stoInodes[isi]

	if !ici.reloadInode(icd, true, func(parentPath string, parentDir *os.File, parentFI os.FileInfo) {

		if fsErr = os.Symlink(target, fmt.Sprintf("%s/%s", parentPath, name)); fsErr != nil {
			return
		}

		if ici.children != nil { // clear content, keep capacity
			ici.children = ici.children[:0]
		}
		cFIs, err := parentDir.Readdir(0)
		if err != nil {
			panic(err)
		} else {
			if cap(ici.children) < len(cFIs) {
				ici.children = make([]InodeID, 0, len(cFIs))
			}
			for _, cfi := range cFIs {

				cici := icd.loadInode(cfi, fmt.Sprintf("%s/%s", parentPath, cfi.Name()))

				if cici == nil { // most prolly a nested mount point
					// keep it invisible to JDFS client
					continue
				}

				if cfi.Name() == name {
					ce = &vfs.ChildInodeEntry{
						Child:                cici.iMeta.inode,
						Generation:           0,
						Attributes:           cici.iMeta.attrs,
						AttributesExpiration: time.Now().Add(vfs.META_ATTRS_CACHE_TIME),
						EntryExpiration:      time.Now().Add(vfs.DIR_CHILDREN_CACHE_TIME),
					}
				}

				ici.children = append(ici.children, cici.inode)
			}
		}

	}) {
		glog.Warningf("inode [%v] lost", ici.inode)
		fsErr = vfs.ENOENT
	}

	return
}

func (icd *icFSD) CreateLink(parent InodeID, name string, target InodeID) (ce *vfs.ChildInodeEntry, fsErr error) {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	isi, ok := icd.regInode[parent]
	if !ok {
		panic(errors.Errorf("parent inode [%v] not in-core ?!", parent))
	}
	ici := &icd.stoInodes[isi]

	isiTarget, ok := icd.regInode[target]
	if !ok {
		panic(errors.Errorf("target inode [%v] not in-core ?!", target))
	}
	iciTarget := &icd.stoInodes[isiTarget]

	if !ici.reloadInode(icd, true, func(parentPath string, parentDir *os.File, parentFI os.FileInfo) {
		if !iciTarget.reloadInode(icd, false, func(targetPath string, targetDir *os.File, targetFI os.FileInfo) {

			if fsErr = os.Link(targetPath, fmt.Sprintf("%s/%s", parentPath, name)); fsErr != nil {
				return
			}

			if ici.children != nil { // clear content, keep capacity
				ici.children = ici.children[:0]
			}
			cFIs, err := parentDir.Readdir(0)
			if err != nil {
				panic(err)
			} else {
				if cap(ici.children) < len(cFIs) {
					ici.children = make([]InodeID, 0, len(cFIs))
				}
				for _, cfi := range cFIs {

					cici := icd.loadInode(cfi, fmt.Sprintf("%s/%s", parentPath, cfi.Name()))

					if cici == nil { // most prolly a nested mount point
						// keep it invisible to JDFS client
						continue
					}

					if cfi.Name() == name {
						ce = &vfs.ChildInodeEntry{
							Child:                cici.iMeta.inode,
							Generation:           0,
							Attributes:           cici.iMeta.attrs,
							AttributesExpiration: time.Now().Add(vfs.META_ATTRS_CACHE_TIME),
							EntryExpiration:      time.Now().Add(vfs.DIR_CHILDREN_CACHE_TIME),
						}
					}

					ici.children = append(ici.children, cici.inode)
				}
			}

		}) {
			glog.Warningf("inode [%v] lost", iciTarget.inode)
			fsErr = vfs.ENOENT
		}
	}) {
		glog.Warningf("inode [%v] lost", ici.inode)
		fsErr = vfs.ENOENT
	}

	return
}

func (icd *icFSD) Rename(oldParent InodeID, oldName string, newParent InodeID, newName string) (fsErr error) {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	isiOldDir, ok := icd.regInode[oldParent]
	if !ok {
		panic(errors.Errorf("parent inode [%v] not in-core ?!", oldParent))
	}
	iciOldDir := &icd.stoInodes[isiOldDir]

	isiNewDir, ok := icd.regInode[oldParent]
	if !ok {
		panic(errors.Errorf("parent inode [%v] not in-core ?!", oldParent))
	}
	iciNewDir := &icd.stoInodes[isiNewDir]

	if !iciOldDir.reloadInode(icd, true, func(oldPath string, oldDir *os.File, oldFI os.FileInfo) {
		if !iciNewDir.reloadInode(icd, true, func(newPath string, newDir *os.File, newFI os.FileInfo) {

			if fsErr = os.Rename(
				fmt.Sprintf("%s/%s", oldPath, oldName),
				fmt.Sprintf("%s/%s", newPath, newName),
			); fsErr != nil {
				return
			}

		}) {
			glog.Warningf("inode [%v] lost", iciNewDir.inode)
			fsErr = vfs.ENOENT
		}
	}) {
		glog.Warningf("inode [%v] lost", iciOldDir.inode)
		fsErr = vfs.ENOENT
	}

	return
}
