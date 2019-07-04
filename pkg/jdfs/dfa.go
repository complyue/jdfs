package jdfs

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/complyue/hbi"

	"github.com/complyue/jdfs/pkg/vfs"

	"github.com/golang/glog"
)

// direct data file access methods

func listJDF(dir string, lb *vfs.DataFileListBuilder, metaExt, dataExt string) {
	if len(dir) <= 0 {
		dir = "."
	}

	df, err := os.OpenFile(dir, os.O_RDONLY, 0)
	if err != nil {
		glog.Errorf("LSDF failed opening dir [%s]:[%s] - %+v", jdfsRootPath, dir, err)
		return
	}
	childFIs, err := df.Readdir(0)
	df.Close()
	if err != nil {
		glog.Errorf("LSDF failed reading dir [%s]:[%s] - %+v", jdfsRootPath, dir, err)
		return
	}

	var subdirList []string
	var metaList []string
	dataSizes := make(map[string]int64)
	for _, childFI := range childFIs {
		fn := childFI.Name()
		if childFI.IsDir() {
			// a dir
			subdirList = append(subdirList, fn)
		} else if childFI.Mode().IsRegular() {
			// a regular file
			if strings.HasSuffix(fn, metaExt) {
				dfPath := fn[:len(fn)-len(metaExt)]
				if dir != "." {
					dfPath = dir + "/" + dfPath
				}
				metaList = append(metaList, dfPath)
			} else if strings.HasSuffix(fn, dataExt) {
				dfPath := fn[:len(fn)-len(dataExt)]
				if dir != "." {
					dfPath = dir + "/" + dfPath
				}
				dataSizes[dfPath] = childFI.Size()
			}
		} else if (childFI.Mode() & os.ModeSymlink) != 0 {
			// a symlink
			// TODO follow or not ?
		} else {
			// a file not reigned by JDFS
			continue
		}
	}

	for _, dfPath := range metaList {
		if size, ok := dataSizes[dfPath]; ok {
			lb.Add(dfPath, size)
		}
	}

	for _, subdir := range subdirList {
		dfPath := subdir
		if dir != "." {
			dfPath = fmt.Sprintf("%s/%s", dir, subdir)
		}
		listJDF(dfPath, lb, metaExt, dataExt)
	}
}

func (efs *exportedFileSystem) ListJDF(rootDir string, metaExt, dataExt string) {
	co := efs.ho.Co()
	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	var lb vfs.DataFileListBuilder
	listJDF(rootDir, &lb, metaExt, dataExt)
	listLen, pathFlatLen, payload := lb.ToSend()

	if err := co.StartSend(); err != nil {
		panic(err)
	}
	if err := co.SendObj(hbi.Repr(listLen)); err != nil {
		panic(err)
	}
	if listLen <= 0 {
		return
	}
	if err := co.SendObj(hbi.Repr(pathFlatLen)); err != nil {
		panic(err)
	}
	i := 0
	if err := co.SendStream(func() ([]byte, error) {
		for i < len(payload) {
			buf := payload[i]
			i++
			if len(buf) > 0 {
				return buf, nil
			}
		}
		return nil, nil
	}); err != nil {
		panic(err)
	}
}

func (efs *exportedFileSystem) AllocJDF(jdfPath string,
	replaceExisting bool,
	metaExt, dataExt string, metaSize int32, dataSize uintptr) {
	co := efs.ho.Co()

	var metaBuf []byte
	if metaSize > 0 {
		metaBuf = efs.bufPool.Get(int(metaSize))
		defer efs.bufPool.Return(metaBuf)
		if err := co.RecvData(metaBuf); err != nil {
			panic(err)
		}
	}

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	var handle vfs.DataFileHandle
	fse := vfs.FsErr(func() (err error) {
		// try best to have parent dir exist, but ignore error here,
		// if parent dir can not be created, file creation will raise
		// error and will be reported.
		os.MkdirAll(filepath.Dir(jdfPath), 0750)

		mfPath := jdfPath + metaExt
		if replaceExisting { // remove existing and ignore error - esp. ENOENT
			syscall.Unlink(mfPath)
		}
		if err = ioutil.WriteFile(mfPath, metaBuf, 0644); err != nil {
			return
		}

		dfPath := jdfPath + dataExt
		if replaceExisting { // remove existing and ignore error - esp. ENOENT
			syscall.Unlink(dfPath)
		}
		var f *os.File
		f, err = os.OpenFile(dfPath, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return
		}
		defer func() {
			if err != nil {
				f.Close()
			}
		}()
		if err = syscall.Ftruncate(int(f.Fd()), int64(dataSize)); err != nil {
			return
		}

		handle, err = efs.dfd.CreateFileHandle(jdfPath, metaExt, dataExt, f)
		if err != nil {
			return
		}
		return
	}())

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	if err := co.SendObj(fse.Repr()); err != nil {
		panic(err)
	}
	if fse != 0 {
		return
	}

	if err := co.SendObj(hbi.Repr(handle)); err != nil {
		panic(err)
	}
}

func (efs *exportedFileSystem) OpenJDF(jdfPath string, metaExt, dataExt string) {
	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	var metaBuf []byte
	var dataSize int64
	var handle vfs.DataFileHandle
	fse := vfs.FsErr(func() (err error) {
		mfPath := jdfPath + metaExt
		metaBuf, err = ioutil.ReadFile(mfPath)
		if err != nil {
			return
		}

		dfPath := jdfPath + dataExt
		var f *os.File
		f, err = os.OpenFile(dfPath, os.O_RDWR, 0644)
		if err != nil {
			return
		}
		defer func() {
			if err != nil {
				f.Close()
			}
		}()

		dataSize, err = f.Seek(0, 2)
		if err != nil {
			return
		}

		handle, err = efs.dfd.CreateFileHandle(jdfPath, metaExt, dataExt, f)
		if err != nil {
			return
		}
		return
	}())

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	if err := co.SendObj(fse.Repr()); err != nil {
		panic(err)
	}
	if fse != 0 {
		return
	}

	if err := co.SendObj(hbi.Repr(len(metaBuf))); err != nil {
		panic(err)
	}
	if len(metaBuf) > 0 {
		if err := co.SendData(metaBuf); err != nil {
			panic(err)
		}
	}

	if err := co.SendObj(hbi.Repr(dataSize)); err != nil {
		panic(err)
	}

	if err := co.SendObj(hbi.Repr(handle)); err != nil {
		panic(err)
	}
}

func (efs *exportedFileSystem) ReadJDF(handle vfs.DataFileHandle,
	dataOffset, dataSize uintptr) {
	co := efs.ho.Co()

	buf := efs.bufPool.Get(int(dataSize))
	defer efs.bufPool.Return(buf)

	// do this before the underlying HBI wire released
	dfh, err := efs.dfd.GetFileHandle(handle, 1)
	if err != nil {
		panic(err)
	}
	fse := vfs.FsErr(func() (err error) {
		defer efs.dfd.FileHandleOpDone(dfh)

		if err := co.FinishRecv(); err != nil {
			panic(err)
		}

		var bytesRead int
		bytesRead, err = dfh.f.ReadAt(buf, int64(dataOffset))
		if err != nil {
			if err == io.EOF {
				// eof is of no interest to ddf consumers,
				// they should conciously manage size of data files.
				err = nil
			} else {
				glog.Errorf("Error reading data file [%d] [%s]:[%s] with handle %d - %+v",
					dfh.inode, jdfsRootPath, dfh.f.Name(), handle, err)
				return
			}
		}

		buf = buf[:bytesRead]

		if glog.V(2) {
			glog.Infof("Read %d bytes @%d from data file [%d] [%s]:[%s] with handle %d",
				bytesRead, dataOffset, dfh.inode, jdfsRootPath, dfh.f.Name(), handle)
		}
		return
	}())

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	if err = co.SendObj(fse.Repr()); err != nil {
		panic(err)
	}
	if fse != 0 {
		return
	}

	if err := co.SendObj(hbi.Repr(len(buf))); err != nil {
		panic(err)
	}
	if len(buf) > 0 {
		if err := co.SendData(buf); err != nil {
			panic(err)
		}
	}
}

func (efs *exportedFileSystem) WriteJDF(handle vfs.DataFileHandle,
	dataOffset, dataSize uintptr) {
	co := efs.ho.Co()

	buf := efs.bufPool.Get(int(dataSize))
	defer efs.bufPool.Return(buf)

	if err := co.RecvData(buf); err != nil {
		panic(err)
	}

	dfh, err := efs.dfd.GetFileHandle(handle, 1)
	if err != nil {
		panic(err)
	}
	fse := vfs.FsErr(func() (err error) {
		// do this before the underlying HBI wire released
		defer efs.dfd.FileHandleOpDone(dfh)

		if err := co.FinishRecv(); err != nil {
			panic(err)
		}

		var bytesWritten int
		bytesWritten, err = dfh.f.WriteAt(buf, int64(dataOffset))
		if err != nil {
			glog.Errorf("Error writing data file [%d] [%s]:[%s] with handle %d - %+v",
				dfh.inode, jdfsRootPath, dfh.f.Name(), handle, err)
			return
		}

		if glog.V(2) {
			glog.Infof("Wrote %d bytes @%d to data file [%d] [%s]:[%s] with handle %d",
				bytesWritten, dataOffset, dfh.inode, jdfsRootPath, dfh.f.Name(), handle)
		}
		return
	}())

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	if err := co.SendObj(fse.Repr()); err != nil {
		panic(err)
	}
	if fse != 0 {
		return
	}

	// todo send bytesWritten back ?
}

func (efs *exportedFileSystem) ExtendJDF(handle vfs.DataFileHandle, dataSize uintptr) {
	co := efs.ho.Co()

	// do this before the underlying HBI wire released
	dfh, err := efs.dfd.GetFileHandle(handle, 1)
	if err != nil {
		panic(err)
	}
	fse := vfs.FsErr(func() (err error) {
		defer efs.dfd.FileHandleOpDone(dfh)

		if err := co.FinishRecv(); err != nil {
			panic(err)
		}

		if err = syscall.Ftruncate(int(dfh.f.Fd()), int64(dataSize)); err != nil {
			glog.Errorf("Error extending data file [%d] [%s]:[%s] to [%d] with handle %d - %+v",
				dfh.inode, jdfsRootPath, dfh.f.Name(), dataSize, handle, err)
			return
		}

		if glog.V(2) {
			glog.Infof("Extended data file [%d] [%s]:[%s] to [%d] with handle %d", dfh.inode,
				jdfsRootPath, dfh.f.Name(), dataSize, handle)
		}
		return
	}())

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	if err := co.SendObj(fse.Repr()); err != nil {
		panic(err)
	}
	if fse != 0 {
		return
	}
}

func (efs *exportedFileSystem) SyncJDF(handle vfs.DataFileHandle) {
	co := efs.ho.Co()

	// do this before the underlying HBI wire released
	dfh, err := efs.dfd.GetFileHandle(handle, 1)
	if err != nil {
		panic(err)
	}
	fse := vfs.FsErr(func() (err error) {
		defer efs.dfd.FileHandleOpDone(dfh)

		if err := co.FinishRecv(); err != nil {
			panic(err)
		}

		if err = dfh.f.Sync(); err != nil {
			glog.Errorf("Error syncing data file [%d] [%s]:[%s] with handle %d - %+v",
				dfh.inode, jdfsRootPath, dfh.f.Name(), handle, err)
			return
		}

		if glog.V(2) {
			glog.Infof("Sync'ed data file [%d] [%s]:[%s] with handle %d", dfh.inode,
				jdfsRootPath, dfh.f.Name(), handle)
		}
		return
	}())

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	if err := co.SendObj(fse.Repr()); err != nil {
		panic(err)
	}
	if fse != 0 {
		return
	}
}

func (efs *exportedFileSystem) CloseJDF(handle vfs.DataFileHandle) {
	co := efs.ho.Co()

	// do this before the underlying HBI wire released
	inode, f := efs.dfd.ReleaseFileHandle(handle)
	if f == nil {
		glog.Fatal("no file pointer from released file handle ?!")
	}

	defer func() { // don't leak f on FinishRecv() error
		dfPath := f.Name()
		if err := f.Close(); err != nil {
			glog.Errorf("Error on closing jdfs data file [%s]:[%s] - %+v",
				jdfsRootPath, dfPath, err)
		}

		if glog.V(2) {
			glog.Infof("DREL data file handle %d released for file [%d] [%s]:[%s]",
				handle, inode, jdfsRootPath, dfPath)
		}
	}()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}
}
