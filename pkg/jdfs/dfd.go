package jdfs

import (
	"os"
	"sync"

	"github.com/complyue/jdfs/pkg/vfs"

	"github.com/golang/glog"
)

// in-core handle to a data file held open
type dfHandle struct {
	// inode of the data file, must be consistent with f
	inode vfs.InodeID

	// path info about the data file.
	// note the actual meta file and data file might have been unlinked from local
	// fs after this data file handle had been opened.
	jdfPath, metaExt, dataExt string

	// f will be kept open until this handle closed
	f *os.File

	// counter of outstanding operations on this file handle, read/write/sync etc.
	opc *sync.WaitGroup
}

// in-core data file data
//
// this shares jdfsRootPath etc. from icd.
// a process should have only one icd active,
// with its pwd chdir'ed to the mounted jdfsRootPath with icd.init()
type icDFD struct {
	// registry of file handles held open, a file handle value is index into this slice
	fileHandles []dfHandle // flat storage of handles
	freeFHIdxs  []int      // free list of indices into fileHandles

	// guard access to session data structs
	mu sync.Mutex
}

func (dfd *icDFD) init(readOnly bool) error {
	dfd.mu.Lock()
	defer dfd.mu.Unlock()

	dfd.fileHandles = []dfHandle{dfHandle{}} // reserve 0 for nil handle
	dfd.freeFHIdxs = nil

	return nil
}

func (dfd *icDFD) CreateFileHandle(jdfPath, metaExt, dataExt string, f *os.File) (
	handle vfs.DataFileHandle, err error) {
	dfd.mu.Lock()
	defer dfd.mu.Unlock()

	var fi os.FileInfo
	if fi, err = f.Stat(); err != nil {
		return
	}
	im := fi2im(f.Name(), fi)

	var hsi int
	if nFreeHdls := len(dfd.freeFHIdxs); nFreeHdls > 0 {
		hsi = dfd.freeFHIdxs[nFreeHdls-1]
		dfd.freeFHIdxs = dfd.freeFHIdxs[:nFreeHdls-1]
		dfd.fileHandles[hsi] = dfHandle{
			inode:   im.inode,
			jdfPath: jdfPath, metaExt: metaExt, dataExt: dataExt,
			f:   f,
			opc: new(sync.WaitGroup),
		}
	} else {
		hsi = len(dfd.fileHandles)
		dfd.fileHandles = append(dfd.fileHandles, dfHandle{
			inode:   im.inode,
			jdfPath: jdfPath, metaExt: metaExt, dataExt: dataExt,
			f:   f,
			opc: new(sync.WaitGroup),
		})
	}

	// return this handle
	handle = vfs.DataFileHandle(hsi)

	if glog.V(2) {
		glog.Infof("DFH created data file handle %d for [%d] [%s]:[%s]", handle, im.inode,
			jdfsRootPath, f.Name())
	}

	return
}

func (dfd *icDFD) FileHandleOpDone(icfh *dfHandle) {
	icfh.opc.Done()
}

func (dfd *icDFD) GetFileHandle(handle vfs.DataFileHandle, incOpc int) (icfh *dfHandle, err error) {
	dfd.mu.Lock()
	defer dfd.mu.Unlock()

	// the opc field (as a WaitGroup) can not be copied, must return a pointer
	icfh = &dfd.fileHandles[handle]

	if incOpc > 0 {
		icfh.opc.Add(incOpc) // increase operation counter with mu locked
	}

	return
}

func (dfd *icDFD) ReleaseFileHandle(handle vfs.DataFileHandle) (inode vfs.InodeID, inoF *os.File) {
	dfd.mu.Lock()
	defer dfd.mu.Unlock()

	icfh := &dfd.fileHandles[handle]

	// grab handle data into return values before it gets cleared
	inode, inoF = icfh.inode, icfh.f

	if glog.V(2) {
		glog.Infof("DFH releasing data file handle %d for [%d] [%s]:[%s]", handle, inode,
			jdfsRootPath, inoF.Name())
	}

	// wait all operations done before closing the underlying file, or they'll fail
	icfh.opc.Wait()

	// fill fields with zero values
	*icfh = dfHandle{}

	dfd.freeFHIdxs = append(dfd.freeFHIdxs, int(handle))

	return
}
