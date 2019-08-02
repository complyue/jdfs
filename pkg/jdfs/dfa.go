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

	"github.com/complyue/jdfs/pkg/errors"
	"github.com/complyue/jdfs/pkg/vfs"

	"github.com/golang/glog"
)

// direct data file access methods

func listJDF(dir string, dfl *vfs.DataFileList, metaExt, dataExt string) {

	dir2open := dir
	if len(dir2open) <= 0 {
		dir2open = "."
	}
	df, err := os.OpenFile(dir2open, os.O_RDONLY, 0)
	if err != nil {
		glog.Warningf("LSDF failed opening dir [%s]:[%s] - %+v", jdfsRootPath, dir, err)
		return
	}
	defer df.Close() // hold an ancestor dir open during recursion within it
	childFIs, err := df.Readdir(0)
	if err != nil {
		glog.Errorf("LSDF failed reading dir [%s]:[%s] - %+v", jdfsRootPath, dir, err)
		return
	}

	var subdirList []string
	var metaList []string
	dataSizes := make(map[string]int64)
	for _, childFI := range childFIs {
		fn := childFI.Name()
		if fn[0] == '.' {
			continue // ignore either file or dir started with a dot
		}
		if childFI.IsDir() {
			// a dir
			subdirList = append(subdirList, fn)
		} else if childFI.Mode().IsRegular() {
			// a regular file
			if strings.HasSuffix(fn, metaExt) {
				dfPath := fn[:len(fn)-len(metaExt)]
				if len(dir) > 0 {
					dfPath = dir + "/" + dfPath
				}
				metaList = append(metaList, dfPath)
			} else if strings.HasSuffix(fn, dataExt) {
				dfPath := fn[:len(fn)-len(dataExt)]
				if len(dir) > 0 {
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
			dfl.Add(size, dfPath)
		}
	}

	for _, subdir := range subdirList {
		dfPath := subdir
		if len(dir) > 0 {
			dfPath = fmt.Sprintf("%s/%s", dir, subdir)
		}
		listJDF(dfPath, dfl, metaExt, dataExt)
	}
}

func (efs *exportedFileSystem) ListJDF(rootDir string, metaExt, dataExt string) {
	co := efs.ho.Co()
	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	var dfl vfs.DataFileList
	listJDF(rootDir, &dfl, metaExt, dataExt)
	listLen, pathFlatLen, payload := dfl.ToSend()

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

func (efs *exportedFileSystem) AllocJDF(jdfPath string, replaceExisting bool,
	metaExt, dataExt string, headerSize int, metaSize int32, dfSize uintptr) {
	co := efs.ho.Co()

	var hdrBuf, metaBuf []byte
	hdrBuf = efs.bufPool.Get(headerSize)
	defer efs.bufPool.Return(hdrBuf)
	if err := co.RecvData(hdrBuf); err != nil {
		panic(err)
	}
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
		if err = syscall.Ftruncate(int(f.Fd()), int64(dfSize)); err != nil {
			return
		}
		var bytesWritten int
		if bytesWritten, err = f.WriteAt(hdrBuf, 0); err != nil {
			return
		} else if bytesWritten != headerSize {
			err = errors.Errorf("Partial header [%d/%d] written!", bytesWritten, headerSize)
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

	if err := co.SendObj(fmt.Sprintf(`[%d,%d]`, handle.Handle, handle.Inode)); err != nil {
		panic(err)
	}
}

func (efs *exportedFileSystem) OpenJDF(jdfPath string, headerBytes int,
	metaExt, dataExt string) {
	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	var hdrBuf []byte
	var metaBuf []byte
	var dfSize int64
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
		var fi os.FileInfo
		if fi, err = f.Stat(); err != nil {
			return
		}
		im := fi2im(dfPath, fi)

		if headerBytes > 0 {
			hdrBuf = efs.bufPool.Get(headerBytes)
			defer efs.bufPool.Return(hdrBuf)
			var hdrReadBytes int
			if hdrReadBytes, err = f.ReadAt(hdrBuf, 0); err != nil {
				glog.Errorf("Error reading header of data file [%d] [%s]:[%s] with handle %d - %+v",
					im.inode, jdfsRootPath, f.Name(), handle, err)
				return
			} else if hdrReadBytes != headerBytes {
				glog.Warningf("Partial header [%d/%d] read from data file [%d] [%s]:[%s] with handle %d",
					hdrReadBytes, headerBytes, im.inode, jdfsRootPath, f.Name(), handle)
			}
		}

		dfSize, err = f.Seek(0, 2)
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

	if headerBytes > 0 {
		if err := co.SendData(hdrBuf); err != nil {
			panic(err)
		}
	}

	if err := co.SendObj(hbi.Repr(len(metaBuf))); err != nil {
		panic(err)
	}
	if len(metaBuf) > 0 {
		if err := co.SendData(metaBuf); err != nil {
			panic(err)
		}
	}

	if err := co.SendObj(hbi.Repr(dfSize)); err != nil {
		panic(err)
	}

	if err := co.SendObj(fmt.Sprintf(`[%d,%d]`, handle.Handle, handle.Inode)); err != nil {
		panic(err)
	}
}

func (efs *exportedFileSystem) StatJDF(jdfPath string, metaExt, dataExt string) {
	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	var dfSize int64
	var inode vfs.InodeID
	fse := vfs.FsErr(func() (err error) {
		// todo not checking meta file for now, need to in the future ?

		dfPath := jdfPath + dataExt
		var f *os.File
		f, err = os.OpenFile(dfPath, os.O_RDWR, 0644)
		if err != nil {
			return
		}
		defer f.Close()

		var fi os.FileInfo
		if fi, err = f.Stat(); err != nil {
			return
		}
		im := fi2im(dfPath, fi)
		inode = im.inode

		dfSize, err = f.Seek(0, 2)
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

	if err := co.SendObj(hbi.Repr(inode)); err != nil {
		panic(err)
	}
	if err := co.SendObj(hbi.Repr(dfSize)); err != nil {
		panic(err)
	}
}

func (efs *exportedFileSystem) ReadJDF(handle int, inode vfs.InodeID,
	dataOffset, dataSize uintptr) {
	co := efs.ho.Co()

	buf := efs.bufPool.Get(int(dataSize))
	defer efs.bufPool.Return(buf)

	// do this before the underlying HBI wire released
	dfh, err := efs.dfd.GetFileHandle(vfs.DataFileHandle{handle, inode}, 1)
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

func (efs *exportedFileSystem) WriteJDF(handle int, inode vfs.InodeID,
	dataOffset, dataSize uintptr) {
	co := efs.ho.Co()

	buf := efs.bufPool.Get(int(dataSize))
	defer efs.bufPool.Return(buf)

	if err := co.RecvData(buf); err != nil {
		panic(err)
	}

	dfh, err := efs.dfd.GetFileHandle(vfs.DataFileHandle{handle, inode}, 1)
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

func (efs *exportedFileSystem) SyncJDF(handle int, inode vfs.InodeID) {
	co := efs.ho.Co()

	// do this before the underlying HBI wire released
	dfh, err := efs.dfd.GetFileHandle(vfs.DataFileHandle{handle, inode}, 1)
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

func (efs *exportedFileSystem) CloseJDF(handle int, inode vfs.InodeID) {
	co := efs.ho.Co()

	// don't let file handle releasing hog the wire
	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	f := efs.dfd.ReleaseFileHandle(vfs.DataFileHandle{handle, inode})
	if f == nil {
		glog.Fatal("no file pointer from released file handle ?!")
		return
	}

	dfPath := f.Name()
	if err := f.Close(); err != nil {
		glog.Errorf("Error on closing jdfs data file [%s]:[%s] - %+v",
			jdfsRootPath, dfPath, err)
	}

	if glog.V(2) {
		glog.Infof("DREL data file handle %d released for file [%d] [%s]:[%s]",
			handle, inode, jdfsRootPath, dfPath)
	}
}
