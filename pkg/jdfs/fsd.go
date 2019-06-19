package jdfs

import (
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/complyue/jdfs/pkg/errors"
	"github.com/complyue/jdfs/pkg/vfs"
	"github.com/golang/glog"
)

var (
	// effective uid/gid of jdfs process, this is told to jdfc when initially
	// mounted, jdfc is supposed to translate all inode owner uid/gid of these values
	// to its FUSE uid/gid as exposed to client kernel/applications, so the owning uid/gid of
	// inodes stored in the backing fs at jdfs can be different from the FUSE uid/gid
	// at jdfc, while those files/dirs appear owned by the FUSE uid/gid.
	//
	// TODO decide handling of uid/gid other than these values, to leave them as is, or
	//      maybe a good idea to translate to a fixed value (e.g. 0=root, 1=daemon) ?
	jdfsUID, jdfsGID uint32

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
	isi int

	jdfPath string

	entries []vfs.DirEnt
}

// in-core handle to a regular file held open
type icfHandle struct {
	isi int

	inode vfs.InodeID

	// f will be kept open until the handle closed
	f *os.File

	// count of outstanding operations on the file, read/write/sync etc.
	opc sync.WaitGroup
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
	jdfsUID = uint32(os.Geteuid())
	jdfsGID = uint32(os.Getegid())

	if err := os.Chdir(jdfsRootPath); err != nil {
		return errors.Errorf("Error chdir to jdfs path: [%s] - %+v", jdfsRootPath, err)
	}
	rootFI, err := os.Lstat(".")
	if err != nil {
		return errors.Errorf("Bad jdfs path: [%s] - %+v", jdfsRootPath, err)
	}
	// dir can only be opened readonly
	rootDir, err := os.OpenFile(".", os.O_RDONLY, 0)
	if err != nil {
		return errors.Errorf("Error open jdfs path: [%s] - %+v", jdfsRootPath, err)
	}
	if !readOnly {
		// TODO test JDFS mount root dir writable
	}

	rootM := fi2im(".", rootFI)

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
	icd.dirHandles = []icdHandle{icdHandle{}} // reserve 0 for nil handle
	icd.freeDHIdxs = nil
	icd.fileHandles = []icfHandle{icfHandle{}} // reserve 0 for nil handle
	icd.freeFHIdxs = nil

	// fake mounted JDFS root inode to be constant 1
	rootM.inode = vfs.RootInodeID

	isi := icd.loadInode(1, rootM, nil, nil, time.Now())
	if isi != 0 {
		panic("root inode got isi other than zero ?!?")
	}

	return nil
}

// must have icd.mu locked
func (icd *icFSD) loadInode(incRef int, im iMeta,
	outdatedPaths []string, children map[string]vfs.InodeID,
	checkTime time.Time) (isi int) {
	jdfPath := im.jdfPath
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
	icd.regInodes[im.inode] = isi

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

func (icd *icFSD) ForgetInode(inode vfs.InodeID, n int) (refcnt int) {
	if inode == vfs.RootInodeID {
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
		return ici.refcnt // still referenced
	}

	delete(icd.regInodes, inode)
	icd.stoInodes[isi] = icInode{} // fill all fields with zero values
	icd.freeInoIdxs = append(icd.freeInoIdxs, isi)

	return 0
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

func (icd *icFSD) CreateDirHandle(inode vfs.InodeID, entries []vfs.DirEnt) (handle vfs.HandleID, err error) {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	isi, ok := icd.regInodes[inode]
	if !ok {
		glog.V(1).Infof("inode not in-core [%v]", inode)
		err = vfs.ENOENT
		return
	}
	ici := &icd.stoInodes[isi]
	if ici.inode != inode {
		glog.Errorf("inode disappeared [%v] ?!", inode)
		err = vfs.ENOENT
		return
	}

	var hsi int
	if nFreeHdls := len(icd.freeDHIdxs); nFreeHdls > 0 {
		hsi = icd.freeDHIdxs[nFreeHdls-1]
		icd.freeDHIdxs = icd.freeDHIdxs[:nFreeHdls-1]
		icd.dirHandles[hsi] = icdHandle{
			isi: isi, jdfPath: ici.reachedThrough[0], entries: entries,
		}
	} else {
		hsi = len(icd.dirHandles)
		icd.dirHandles = append(icd.dirHandles, icdHandle{
			isi: isi, jdfPath: ici.reachedThrough[0], entries: entries,
		})
	}
	handle = vfs.HandleID(hsi)

	return
}

func (icd *icFSD) GetDirHandle(inode vfs.InodeID, handle int) (icdh icdHandle, err error) {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	// snapshot the value instead of getting a pointer, tho it's unlikely the handle be
	// destroyed before read, but just in case.
	icdh = icd.dirHandles[handle]

	if icdh.isi < 0 { // isi 0 is root dir, possible to be an opened dir,
		// released handles will have isi filled with -1
		err = vfs.ENOENT
		return
	}

	ici := &icd.stoInodes[icdh.isi]
	if ici.inode != inode {
		err = syscall.ESTALE // TODO fuse kernel is happy with this ?
		return
	}

	return
}

func (icd *icFSD) ReleaseDirHandle(handle int) (released icdHandle) {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	icdh := &icd.dirHandles[handle]
	released = *icdh // snapshot a copy to return

	if icdh.isi < 0 {
		panic(errors.New("releasing non-existing dir handle ?!"))
	}

	// fill fields with invalid values
	*icdh = icdHandle{
		isi: -1, // isi 0 is root dir, possible to be an opened dir
	}

	icd.freeDHIdxs = append(icd.freeDHIdxs, handle)

	return
}

func (icd *icFSD) CreateFileHandle(inode vfs.InodeID, inoF *os.File) (handle vfs.HandleID, err error) {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	isi, ok := icd.regInodes[inode]
	if !ok {
		glog.V(1).Infof("inode not in-core [%v]", inode)
		err = vfs.ENOENT
		return
	}
	ici := &icd.stoInodes[isi]
	if ici.inode != inode {
		glog.Errorf("inode disappeared [%v] ?!", inode)
		err = vfs.ENOENT
		return
	}

	var hsi int
	if nFreeHdls := len(icd.freeFHIdxs); nFreeHdls > 0 {
		hsi = icd.freeFHIdxs[nFreeHdls-1]
		icd.freeFHIdxs = icd.freeFHIdxs[:nFreeHdls-1]
		icd.fileHandles[hsi] = icfHandle{
			isi: isi, inode: ici.inode, f: inoF,
		}
	} else {
		hsi = len(icd.fileHandles)
		icd.fileHandles = append(icd.fileHandles, icfHandle{
			isi: isi, inode: ici.inode, f: inoF,
		})
	}
	handle = vfs.HandleID(hsi)

	return
}

func (icd *icFSD) GetFileHandle(inode vfs.InodeID, handle int) (icfh *icfHandle, err error) {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	// the opc field (as a WaitGroup) can not be copied, must return a pointer
	icfh = &icd.fileHandles[handle]

	if icfh.isi <= 0 { // isi 0 is root dir, not possible to be an opened file
		err = vfs.ENOENT
		return
	}

	if inode != 0 { // 0 for no inode to be matched
		ici := &icd.stoInodes[icfh.isi]
		if ici.inode != inode {
			err = syscall.ESTALE // TODO fuse kernel is happy with this ?
			return
		}
	}

	return
}

func (icd *icFSD) ReleaseFileHandle(handle int) {
	icd.mu.Lock()
	defer icd.mu.Unlock()

	icfh := &icd.fileHandles[handle]

	if icfh.isi <= 0 { // isi 0 is root dir, not possible to be an opened file
		panic(errors.New("releasing non-existing file handle ?!"))
	}

	// fill fields with zero values
	*icfh = icfHandle{}

	icd.freeFHIdxs = append(icd.freeFHIdxs, handle)
}
