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
	lastChecked         time.Time
	lastChildrenChecked time.Time

	// cached inode ids of children of a dir.
	// will always be nil for non-dir inode; and will be nil for a dir inode, before it's
	// loaded, or has been forcefully invalidated.
	//
	// if non-nil, the map is per-see at lastChildrenChecked time, and is safe to be read
	// concurrently as it won't be written concurrently.
	//
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

	isi := icd.loadInode(1, rootM, nil, nil, time.Now())
	if isi != 0 {
		panic("root inode got isi other than zero ?!?")
	}
	ici := &icd.stoInodes[isi]

	return nil
}

// must have icd.mu locked
func (icd *icFSD) loadInode(incRef int, im iMeta,
	outdatedPaths []string, children map[string]vfs.InodeID,
	checkTime time.Time) (isi int) {
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

		// the algorithm here may fail to discard some of the outdated paths,
		// but they'll be realized later again anyway, no need to try very hard here.
		for _, outdatedPath := range outdatedPaths {
			rpl := len(ici.reachedThrough)
			if rpl <= 0 {
				break
			}
			if outdatedPath == ici.reachedThrough[rpl-1] {
				ici.reachedThrough = ici.reachedThrough[:rpl-1]
			}
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

		if checkTime.After(ici.lastChecked) {
			// update meta attrs
			ici.attrs = im.attrs
			// update cached children if loaded as well
			if children != nil {
				ici.children = children
				ici.lastChildrenChecked = checkTime
			}
			ici.lastChecked = checkTime
		} else {
			// an early performed fs check op arrived late, ignore
		}

		// apply reference count increment
		ici.refcnt += incRef

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

		refcnt: incRef,

		reachedThrough:      []string{jdfPath},
		lastChecked:         checkTime,
		lastChildrenChecked: checkTime,
		children:            children,
	}

	return
}

// LoadInode loads the specified inode meta data if it is not out-dated,
// and returns the latest snapshot copy of the in-core inode record.
//
// if checkTime != ici.lastChecked, the returned meta data should be more
// recent than supplied.
func (icd *icFSD) LoadInode(incRef int, im iMeta,
	outdatedPaths []string, children map[string]vfs.InodeID,
	checkTime time.Time) (ici icInode, ok bool) {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	isi := icd.loadInode(incRef, im, outdatedPaths, children, checkTime)
	if isi < 0 {
		// ok is false to be returned
		return // situation should have been logged in loadInode()
	}

	// take a snapshot of the inode record when mu locked for return value
	ici, ok = icd.stoInodes[isi], true
	return
}

func (icd *icFSD) InvalidateChildren(inode vfs.InodeID,
	goneName string, comeName string) {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	isi, ok := icd.regInodes[inode]
	if !ok {
		panic(errors.Errorf("inode [%v] not in-core ?!", inode))
	}
	ici := &icd.stoInodes[isi]

	// Note: should NOT modify armed children map, for safe concurrent reading of it

	if len(comeName) > 0 {
		// a new child comes in, invalidate the cache to force a reload next time needed
		ici.children = nil
	} else if len(goneName) > 0 {
		// a child goes away
		// TODO is it worth doing to make a new map with name excluded ?
		//      the children list must be long enough for sure, but how long?
		ici.children = nil
	} else {
		// is this a reasonable case ?
		ici.children = nil
	}
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
