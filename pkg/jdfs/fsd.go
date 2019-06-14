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

// in-core inode info
type icInode struct {
	// meta data of this inode
	inode vfs.InodeID
	attrs vfs.InodeAttributes

	// number of references counted by FUSE
	//
	// when an in-core record's reference count is decreased to zero, it'll be dropped,
	// and all it's children with 0 refcnt will be dropped as well.
	//
	// note: prefetched records will be in-core but have refcnt==0
	refcnt int

	// jdf paths through which this inode has been reached
	reachedThrough []string

	// last time at which attrs/children are refreshed
	lastChecked time.Time

	// cached children of a dir. will always be nil for non-dir inode; and will be nil
	// for a dir after cache is invalidated, if non-nil, the map is per-see at
	// lastChecked time.
	// todo is there needs to preserve directory order? if so an ordered map should be used.
	children map[string]vfs.InodeID
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
// with its pwd chdir'ed to the mounted jdfsRootPath with icd.init()
type icFSD struct {

	// registry of in-core info of inodes
	regInodes   map[vfs.InodeID]int // map inode ID to indices into stoInodes
	stoInodes   []icInode           // flat storage of icInodes
	freeInoIdxs []int               // free list of indices into stoInodes

	// registry of dir handles held open, a dir handle value is index into this slice
	dirHandles []icdHandle // flat storage of handles
	freeDHIdxs []int       // free list of indices into dirHandles

	// registry of file handles held open, a file handle value is index into this slice
	fileHandles []icfHandle // flat storage of handles
	freeFHIdxs  []int       // free list of indices into fileHandles

	// guard access to session data structs
	mu sync.Mutex
}

func (icd *icFSD) init(readOnly bool) error {
	var flags int
	if readOnly {
		flags = os.O_RDONLY
	} else {
		flags = os.O_RDWR
	}
	if err := os.Chdir(jdfsRootPath); err != nil {
		return errors.Errorf("Error chdir to jdfs path: [%s] - %+v", jdfsRootPath, err)
	}
	rootFI, err := os.Lstat(".")
	if err != nil {
		return errors.Errorf("Bad jdfs path: [%s] - %+v", jdfsRootPath, err)
	}
	rootDir, err := os.OpenFile(".", flags, 0)
	if err != nil {
		return errors.Errorf("Error open jdfs path: [%s] - %+v", jdfsRootPath, err)
	}

	rootM := fi2im("", rootFI)

	icd.mu.Lock()
	defer icd.mu.Unlock()

	if jdfRootDir != nil {
		jdfRootDir.Close()
	}
	jdfRootDir = rootDir
	jdfRootDevice = rootM.dev
	jdfRootInode = rootM.inode

	// todo sophisticate initial in-core data allocation,
	// may base on statistics from local fs and config.
	icd.regInodes = make(map[vfs.InodeID]int)
	icd.stoInodes = nil
	icd.freeInoIdxs = nil
	icd.fileHandles = []icfHandle{icfHandle{}} // reserve 0 for nil handle
	icd.freeFHIdxs = nil

	isi := icd.loadInode(rootM)
	if isi != 0 {
		panic("root inode got isi other than zero ?!?")
	}
	ici := &icd.stoInodes[isi]
	ici.refcnt++ // not really needed as root inode won't be forgotten anyway

	return nil
}

// must have icd.mu locked
func (icd *icFSD) loadInode(im iMeta) (isi int) {
	jdfPath := im.jdfPath()
	if im.dev != jdfRootDevice {
		glog.Warningf("Nested mount point [%s] under [%s] not supported by JDFS.",
			jdfPath, jdfsRootPath)
		return -1
	}

	var ok bool
	isi, ok = icd.regInodes[im.inode]
	if ok { // discovered a new hard link to a known inode
		ici := &icd.stoInodes[isi]
		if im.inode != ici.inode {
			panic(errors.New("regInodes corrupted ?!"))
		}

		// record reached through jdfPath
		prevReached := false
		for i := len(ici.reachedThrough) - 1; i >= 0; i-- {
			if ici.reachedThrough[i] == jdfPath {
				prevReached = true
				break
			}
		}
		if !prevReached { // reached from a new path
			ici.reachedThrough = append(ici.reachedThrough, jdfPath)
		}

		// update meta attrs
		ici.attrs = im.attrs
		ici.lastChecked = time.Now()
		return
	}

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
		inode: im.inode, attrs: im.attrs,

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

// LoadInode loads the specified inode meta data, then returns a snapshot copy of the
// in-core inode record, if the load was successful.
func (icd *icFSD) LoadInode(incRef int, im iMeta,
	outdatedPaths []string, children map[string]vfs.InodeID) (ici icInode, ok bool) {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	isi := icd.loadInode(im)
	if isi < 0 {
		return // situation should have been loaded in loadInode()
	}

	icip := &icd.stoInodes[isi]
	if icip.inode != im.inode {
		panic(errors.Errorf("inode [%v] changed to [%v] ?!", im.inode, icip.inode))
	}

	for _, outdatedPath := range outdatedPaths {
		rpl := len(icip.reachedThrough)
		if rpl <= 0 {
			break
		}
		if outdatedPath == icip.reachedThrough[rpl-1] {
			icip.reachedThrough = icip.reachedThrough[:rpl-1]
		}
	}

	if children != nil {
		icip.children = children
	}

	icip.refcnt += incRef
	ici, ok = *icip, true
	return
}

func (icd *icFSD) ForgetInode(inode vfs.InodeID, n int) {
	if inode == jdfRootInode {
		panic(errors.Errorf("forget root ?!"))
	}

	if n <= 0 {
		panic(errors.Errorf("forget %d ref ?!", n))
	}

	icd.mu.Lock()
	defer icd.mu.Unlock()

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

	icd.forgetInode(inode)
}

// must have icd.mu locked
func (icd *icFSD) forgetInode(inode vfs.InodeID) {
	isi, ok := icd.regInodes[inode]
	if !ok {
		panic(errors.Errorf("inode [%v] not in-core ?!", inode))
	}
	ici := &icd.stoInodes[isi]

	if ici.refcnt > 0 {
		return // still referenced
	}

	delete(icd.regInodes, inode)
	icd.stoInodes[isi] = icInode{} // clear all fields to zero values
	icd.freeInoIdxs = append(icd.freeInoIdxs, isi)

	if ici.children == nil {
		return // no children cached
	}

	for _, cInode := range ici.children {
		icd.forgetInode(cInode)
	}
}

// must have icd.mu locked
func (icd *icFSD) getInode(inode vfs.InodeID) *icInode {
	isi, ok := icd.regInodes[inode]
	if !ok {
		glog.V(1).Infof("inode not in-core [%v]", inode)
		return nil
	}
	ici := &icd.stoInodes[isi]
	if ici.inode != inode {
		glog.Errorf("inode disappeared [%v] ?!", inode)
		return nil
	}
	return ici
}

// GetInode returns a snapshot of in-core inode record
func (icd *icFSD) GetInode(incRef int, inode vfs.InodeID) (ici icInode, ok bool) {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	icip := icd.getInode(inode)
	if icip == nil {
		return
	}

	icip.refcnt += incRef
	ici, ok = *icip, true
	return
}

func (icd *icFSD) CreateFile(parent vfs.InodeID, name string, mode uint32) (
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

func (icd *icFSD) CreateSymlink(parent vfs.InodeID, name string, target string) (ce *vfs.ChildInodeEntry, err error) {
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
					Child:                cici.inode,
					Generation:           0,
					Attributes:           cici.attrs,
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

func (icd *icFSD) CreateLink(parent vfs.InodeID, name string, target vfs.InodeID) (ce *vfs.ChildInodeEntry, err error) {
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
							Child:                cici.inode,
							Generation:           0,
							Attributes:           cici.attrs,
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

func (icd *icFSD) Rename(oldParent vfs.InodeID, oldName string, newParent vfs.InodeID, newName string) (err error) {
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

func (icd *icFSD) RmDir(parent vfs.InodeID, name string) (err error) {
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

			if ici.children != nil {
				if cInode, ok := ici.children[name]; ok {
					delete(ici.children, name)
					icd.forgetInode(cInode)
				}
			}

			return
		}); err != nil {
		glog.Warningf("inode lost [%v] - %+v", ici.inode, err)
		return
	}

	return
}

func (icd *icFSD) Unlink(parent vfs.InodeID, name string) (err error) {
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

			if ici.children != nil {
				if cInode, ok := ici.children[name]; ok {
					delete(ici.children, name)
					icd.forgetInode(cInode)
				}
			}

			return
		}); err != nil {
		glog.Warningf("inode lost [%v] - %+v", ici.inode, err)
		return
	}

	return
}

func (icd *icFSD) OpenDir(inode vfs.InodeID) (handle vfs.HandleID, err error) {
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

	// snapshot dir entries at open
	// TODO out-of-core handling necessary ?
	if err = ici.refreshChildren(icd, false, nil, func(childName string, cisi int) {
		cici := &icd.stoInodes[cisi]
		entType := vfs.DT_Unknown
		if cici.attrs.Mode.IsDir() {
			entType = vfs.DT_Directory
		} else if cici.attrs.Mode.IsRegular() {
			entType = vfs.DT_File
		} else if cici.attrs.Mode&os.ModeSymlink != 0 {
			entType = vfs.DT_Link
		} else {
			return // hide this strange inode to jdfc
		}
		entries = append(entries, vfs.DirEnt{
			Offset: vfs.DirOffset(len(entries) + 1),
			Inode:  cici.inode,
			Name:   childName,
			Type:   entType,
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

func (icd *icFSD) ReadDir(inode vfs.InodeID, handle int, offset int, buf []byte) (bytesRead int, err error) {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	icdh := &icd.dirHandles[handle]
	ici := &icd.stoInodes[icdh.isi]
	if ici.inode != inode {
		err = syscall.ESTALE // TODO fuse kernel is happy with this ?
		return
	}

	for i := offset; i < len(icdh.entries); i++ {
		n := vfs.Writevfs.DirEnt(buf[bytesRead:], icdh.entries[i])
		if n <= 0 {
			break
		}
		bytesRead += n
	}

	return
}

func (icd *icFSD) ReleaseDirHandle(handle int) {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	if icd.dirHandles[handle].isi <= 0 {
		panic(errors.New("releasing non-existing dir handle ?!"))
	}

	icd.dirHandles[handle] = icdHandle{} // reset fields to zero values

	icd.freeDHIdxs = append(icd.freeDHIdxs, handle)
}

func (icd *icFSD) OpenFile(inode vfs.InodeID, flags uint32) (handle vfs.HandleID, err error) {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	isi, ok := icd.regInodes[inode]
	if !ok {
		panic(errors.Errorf("inode [%v] not in-core ?!", inode))
	}
	ici := &icd.stoInodes[isi]

	if err = ici.refreshInode(icd, (flags&uint32(os.O_RDWR|os.O_WRONLY)) != 0,
		func(inoPath string, inoF *os.File, inoFI os.FileInfo) (keepF bool, err error) {

			if !inoFI.Mode().IsRegular() {
				err = syscall.EINVAL // TODO fuse kernel happy with this ?
				return
			}

			keepF = true

			var hsi int
			if nFreeHdls := len(icd.freeFHIdxs); nFreeHdls > 0 {
				hsi = icd.freeFHIdxs[nFreeHdls-1]
				icd.freeFHIdxs = icd.freeFHIdxs[:nFreeHdls-1]
				icd.fileHandles[hsi] = icfHandle{
					isi: isi, f: inoF,
				}
			} else {
				hsi = len(icd.fileHandles)
				icd.fileHandles = append(icd.fileHandles, icfHandle{
					isi: isi, f: inoF,
				})
			}
			handle = vfs.HandleID(hsi)

			return
		}); err != nil {
		return
	}

	return
}

func (icd *icFSD) ReadFile(inode vfs.InodeID, handle int, offset int64, buf []byte) (bytesRead int, err error) {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	icfh := &icd.fileHandles[handle]
	ici := &icd.stoInodes[icfh.isi]
	if ici.inode != inode {
		err = syscall.ESTALE // TODO fuse kernel is happy with this ?
		return
	}

	if bytesRead, err = icfh.f.ReadAt(buf, offset); err != nil {
		return
	}

	return
}
