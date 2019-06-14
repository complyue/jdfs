// Package jdfs defines implementation of the Just Data FileSystem server
package jdfs

import (
	"fmt"
	"io"
	"os"
	"syscall"
	"time"
	"unsafe"

	"github.com/complyue/hbi"
	"github.com/complyue/jdfs/pkg/vfs"
)

type exportedFileSystem struct {
	// the root directory that this JDFS server is willing to export.
	//
	// a jdfc can mount jdfPath="/" for this root directory,
	// or it can mount any sub dir under this path.
	//
	// multiple local filesystems can be separately mounted under this path for different
	// jdfc to mount.
	//
	// TODO for a JDFS mount to expose nested filesystems under its mounted root dir,
	// there're possibilities that inode numbers from different fs collide, maybe FUSE
	// generationNumber can be used to support that, or just don't support nested fs over
	// JDFS.
	exportRoot string

	// HBI posting/hosting ends
	po *hbi.PostingEnd
	ho *hbi.HostingEnd

	// effective uid/gid of jdfs process, this is told to jdfc when initially
	// mounted, jdfc is supposed to translate all inode owner uid/gid of these values
	// to its FUSE uid/gid as exposed to client kernel/applications, so the owning uid/gid of
	// inodes stored in the backing fs at jdfs can be different from the FUSE uid/gid
	// at jdfc, while those files/dirs appear owned by the FUSE uid/gid.
	//
	// TODO decide handling of uid/gid other than these values, to leave them as is, or
	//      maybe a good idea to translate to a fixed value (e.g. 0=root, 1=daemon) ?
	jdfsUID, jdfsGID int

	// whether readOnly, as jdfc requested on initial mount
	readOnly bool

	// in-core filesystem data
	icd icFSD

	// buffer pool
	bufPool BufPool
}

func (efs *exportedFileSystem) NamesToExpose() []string {
	return []string{
		"Mount", "StatFS", "LookUpInode", "GetInodeAttributes", "SetInodeAttributes", "ForgetInode",
		"MkDir", "CreateFile", "CreateSymlink", "CreateLink", "Rename", "RmDir", "Unlink",
		"OpenDir", "ReadDir", "ReleaseDirHandle",
	}
}

func (efs *exportedFileSystem) Mount(readOnly bool, jdfPath string) {
	efs.jdfsUID = os.Geteuid()
	efs.jdfsGID = os.Getegid()

	efs.readOnly = readOnly

	var rootPath string
	if jdfPath == "/" || jdfPath == "" {
		rootPath = efs.exportRoot
	} else {
		rootPath = efs.exportRoot + jdfPath
	}

	jdfsRootPath = rootPath
	if err := efs.icd.init(readOnly); err != nil {
		efs.ho.Disconnect(fmt.Sprintf("%s", err), true)
		panic(err)
	}

	co := efs.ho.Co()
	if err := co.StartSend(); err != nil {
		panic(err)
	}

	// send mount result fields
	if err := co.SendObj(hbi.Repr(hbi.LitListType{
		jdfRootInode, efs.jdfsUID, efs.jdfsGID,
	})); err != nil {
		panic(err)
	}
}

func (efs *exportedFileSystem) StatFS() {
	co := efs.ho.Co()

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	var op vfs.StatFSOp

	op, err := statFS(jdfRootDir)
	if err != nil {
		panic(err)
	}

	bufView := ((*[unsafe.Sizeof(op)]byte)(unsafe.Pointer(&op)))[0:unsafe.Sizeof(op)]
	if err := co.SendData(bufView); err != nil {
		panic(err)
	}
}

func (efs *exportedFileSystem) LookUpInode(parent vfs.InodeID, name string) {
	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	if parent == vfs.RootInodeID { // translate FUSE root to actual root inode
		parent = jdfRootInode
	}

	var ce *vfs.ChildInodeEntry

	var fsErr error

	if ici, ok := efs.icd.GetInode(parent); !ok {
		fsErr = vfs.ENOENT
	} else {
		children := ici.children
		if children == nil || time.Now().Sub(ici.lastChecked) > vfs.DIR_CHILDREN_CACHE_TIME {
			// read dir contents from local fs, cache to children list
			if parentM, childMs, outdatedPaths, err := readInodeDir(parent, ici.reachedThrough); err != nil {
				fsErr = err
			} else {
				children = make([string]vfs.InodeID, len(childMs))
				for i := range childMs {
					childM := &childMs[i]
					children[childM.name] = childM.inode

					if childM.name == name {
						if cici, ok := efs.icd.LoadInode(1, fi2im(parentM.jdfPath(), *childM), nil, nil); ok {
							ce = &vfs.ChildInodeEntry{
								Child:                cici.inode,
								Generation:           0,
								Attributes:           cici.attrs,
								AttributesExpiration: time.Now().Add(vfs.META_ATTRS_CACHE_TIME),
								EntryExpiration:      time.Now().Add(vfs.DIR_CHILDREN_CACHE_TIME),
							}
						}
					}
				}
				ici = efs.icd.LoadInode(parentM, outdatedPaths, children)
			}
		} else {
			// use children list cache
			if cInode, ok := children[name]; !ok {
				// no child with specified name
				fsErr = vfs.ENOENT
			} else if cici, ok := efs.icd.GetInode(1, cInode); ok {
				// already in-core
				ce = &vfs.ChildInodeEntry{
					Child:                cici.inode,
					Generation:           0,
					Attributes:           cici.attrs,
					AttributesExpiration: time.Now().Add(vfs.META_ATTRS_CACHE_TIME),
					EntryExpiration:      time.Now().Add(vfs.DIR_CHILDREN_CACHE_TIME),
				}
			} else {
				// not yet in-core, consult local fs
				if parentPath, parentM, outdatedPaths, err := statInode(
					ici.inode, ici.reachedThrough,
				); err != nil {
					fsErr = err
				} else if ici, ok := efs.icd.LoadInode(parentM, outdatedPaths, nil); ok {
					childPath := fmt.Sprintf("%s/%s", parentPath, name)
					if cFI, err := os.Lstat(childPath); err != nil {
						fsErr = err
					} else if cici, ok := efs.icd.LoadInode(1, fi2im("", cFI), nil, nil); ok {
						ce = &vfs.ChildInodeEntry{
							Child:                cici.inode,
							Generation:           0,
							Attributes:           cici.attrs,
							AttributesExpiration: time.Now().Add(vfs.META_ATTRS_CACHE_TIME),
							EntryExpiration:      time.Now().Add(vfs.DIR_CHILDREN_CACHE_TIME),
						}
					}
				}
			}
		}
	}

	if fsErr == nil && ce == nil {
		fsErr = vfs.ENOENT
	}

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	switch fse := fsErr.(type) {
	case nil:
		if err := co.SendObj(`0`); err != nil {
			panic(err)
		}
	case syscall.Errno:
		// TODO assess errno compatibility esp. when jdfs/jdfc run different Arch/OSes
		if err := co.SendObj(hbi.Repr(int(fse))); err != nil {
			panic(err)
		}
		return
	default:
		panic(fsErr)
	}

	bufView := ((*[unsafe.Sizeof(*ce)]byte)(unsafe.Pointer(ce)))[0:unsafe.Sizeof(*ce)]
	if err := co.SendData(bufView); err != nil {
		panic(err)
	}
}

func (efs *exportedFileSystem) GetInodeAttributes(inode vfs.InodeID) {
	co := efs.ho.Co()
	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	if inode == vfs.RootInodeID { // translate FUSE root to actual root inode
		inode = jdfRootInode
	}

	var attrs vfs.InodeAttributes

	var fsErr error

	if ici, ok := efs.icd.GetInode(inode); !ok {
		fsErr = vfs.ENOENT
	} else if time.Now().Sub(ici.lastChecked) > vfs.META_ATTRS_CACHE_TIME {
		// refresh after cache time out
		if _, inoM, outdatedPaths, err := statInode(
			ici.inode, ici.reachedThrough,
		); err != nil {
			fsErr = err
		} else if ici, ok = efs.icd.LoadInode(0, inoM, outdatedPaths, nil); !ok {
			fsErr = vfs.ENOENT
		} else {
			attrs = ici.attrs
		}
	} else {
		// use cached attrs
		attrs = ici.attrs
	}

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	switch fse := fsErr.(type) {
	case nil:
		if err := co.SendObj(`0`); err != nil {
			panic(err)
		}
	case syscall.Errno:
		// TODO assess errno compatibility esp. when jdfs/jdfc run different Arch/OSes
		if err := co.SendObj(hbi.Repr(int(fse))); err != nil {
			panic(err)
		}
		return
	default:
		panic(fsErr)
	}

	bufView := ((*[unsafe.Sizeof(attrs)]byte)(unsafe.Pointer(&attrs)))[0:unsafe.Sizeof(attrs)]
	if err := co.SendData(bufView); err != nil {
		panic(err)
	}
}

func (efs *exportedFileSystem) SetInodeAttributes(inode vfs.InodeID,
	chgSize, chgMode, chgMtime bool,
	sz uint64, mode uint32, mNsec int64,
) {
	co := efs.ho.Co()
	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	if inode == vfs.RootInodeID { // translate FUSE root to actual root inode
		inode = jdfRootInode
	}

	var attrs vfs.InodeAttributes

	var fsErr error

	if ici, ok := efs.icd.GetInode(inode); !ok {
		fsErr = vfs.ENOENT
	} else if jdfPath, inoM, outdatedPaths, err := statInode(
		ici.inode, ici.reachedThrough,
	); err != nil {
		fsErr = err
	} else if inoF, err := os.OpenFile(jdfPath, os.O_RDWR, 0); err != nil {
		fsErr = err
	} else {
		defer inoF.Close()

		if chgSize && fsErr == nil {
			fsErr = inoF.Truncate(int64(sz))
		}

		if chgMode && fsErr == nil {
			fsErr = inoF.Chmod(os.FileMode(mode))
		}

		if chgMtime && fsErr == nil {
			fsErr = chftimes(inoF, mNsec)
		}

		if fsErr != nil { // stat local fs again for new meta attrs
			if inoFI, err := os.Lstat(jdfPath); err != nil {
				fsErr = err
			} else if ici, ok := efs.icd.LoadInode(fi2im("", inoFI), outdatedPaths, nil); !ok {
				fsErr = vfs.ENOENT
			} else {
				attrs = ici.attrs
			}
		}

	}

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	switch fse := fsErr.(type) {
	case nil:
		if err := co.SendObj(`0`); err != nil {
			panic(err)
		}
	case syscall.Errno:
		// TODO assess errno compatibility esp. when jdfs/jdfc run different Arch/OSes
		if err := co.SendObj(hbi.Repr(int(fse))); err != nil {
			panic(err)
		}
		return
	default:
		panic(fsErr)
	}

	bufView := ((*[unsafe.Sizeof(attrs)]byte)(unsafe.Pointer(&attrs)))[0:unsafe.Sizeof(attrs)]
	if err := co.SendData(bufView); err != nil {
		panic(err)
	}
}

func (efs *exportedFileSystem) ForgetInode(inode vfs.InodeID, n int) {
	if inode == vfs.RootInodeID || inode == jdfRootInode {
		return // never forget about root
	}

	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	efs.icd.ForgetInode(inode, n)
}

func (efs *exportedFileSystem) MkDir(parent vfs.InodeID, name string, mode uint32) {
	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	var ce *vfs.ChildInodeEntry

	var fsErr error

	if ici, ok := efs.icd.GetInode(parent); !ok {
		fsErr = vfs.ENOENT
	} else if parentPath, parentM, outdatedPaths, err := statInode(
		ici.inode, ici.reachedThrough,
	); err != nil {
		fsErr = err
	} else {
		childPath := fmt.Sprintf("%s/%s", parentPath, name)
		if err := os.Mkdir(childPath, os.FileMode(mode)); err != nil {
			fsErr = err
		} else if cFI, err := os.Lstat(childPath); err != nil {
			fsErr = err
		} else if cici, ok := efs.icd.LoadInode(fi2im("", cFI), nil, nil); !ok {
			fsErr = vfs.ENOENT
		} else {
			ce = &vfs.ChildInodeEntry{
				Child:                cici.inode,
				Generation:           0,
				Attributes:           cici.attrs,
				AttributesExpiration: time.Now().Add(vfs.META_ATTRS_CACHE_TIME),
				EntryExpiration:      time.Now().Add(vfs.DIR_CHILDREN_CACHE_TIME),
			}
		}
	}

	if fsErr == nil && ce == nil {
		fsErr = vfs.ENOENT
	}

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	switch fse := fsErr.(type) {
	case nil:
		if err := co.SendObj(`0`); err != nil {
			panic(err)
		}
	case syscall.Errno:
		if err := co.SendObj(hbi.Repr(int(fse))); err != nil {
			panic(err)
		}
		return
	default:
		panic(fsErr)
	}

	bufView := ((*[unsafe.Sizeof(*ce)]byte)(unsafe.Pointer(ce)))[0:unsafe.Sizeof(*ce)]
	if err := co.SendData(bufView); err != nil {
		panic(err)
	}
}

func (efs *exportedFileSystem) CreateFile(parent vfs.InodeID, name string, mode uint32) {
	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	ce, handle, fsErr := efs.icd.CreateFile(parent, name, mode)

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	switch fse := fsErr.(type) {
	case nil:
		if ce == nil {
			// TODO elaborate error description and handling by jdfc in this case
			if err := co.SendObj(hbi.Repr(int(vfs.EEXIST))); err != nil {
				panic(err)
			}
			return
		}
		if err := co.SendObj(`0`); err != nil {
			panic(err)
		}
	case syscall.Errno:
		if err := co.SendObj(hbi.Repr(int(fse))); err != nil {
			panic(err)
		}
		return
	default:
		panic(fsErr)
	}

	if err := co.SendObj(hbi.Repr(int(handle))); err != nil {
		panic(err)
	}

	bufView := ((*[unsafe.Sizeof(*ce)]byte)(unsafe.Pointer(ce)))[0:unsafe.Sizeof(*ce)]
	if err := co.SendData(bufView); err != nil {
		panic(err)
	}
}

func (efs *exportedFileSystem) CreateSymlink(parent vfs.InodeID, name string, target string) {
	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	ce, fsErr := efs.icd.CreateSymlink(parent, name, target)

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	switch fse := fsErr.(type) {
	case nil:
		if ce == nil {
			// TODO elaborate error description and handling by jdfc in this case
			if err := co.SendObj(hbi.Repr(int(vfs.EEXIST))); err != nil {
				panic(err)
			}
			return
		}
		if err := co.SendObj(`0`); err != nil {
			panic(err)
		}
	case syscall.Errno:
		if err := co.SendObj(hbi.Repr(int(fse))); err != nil {
			panic(err)
		}
		return
	default:
		panic(fsErr)
	}

	bufView := ((*[unsafe.Sizeof(*ce)]byte)(unsafe.Pointer(ce)))[0:unsafe.Sizeof(*ce)]
	if err := co.SendData(bufView); err != nil {
		panic(err)
	}
}

func (efs *exportedFileSystem) CreateLink(parent vfs.InodeID, name string, target vfs.InodeID) {
	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	ce, fsErr := efs.icd.CreateLink(parent, name, target)

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	switch fse := fsErr.(type) {
	case nil:
		if ce == nil {
			// TODO elaborate error description and handling by jdfc in this case
			if err := co.SendObj(hbi.Repr(int(vfs.EEXIST))); err != nil {
				panic(err)
			}
			return
		}
		if err := co.SendObj(`0`); err != nil {
			panic(err)
		}
	case syscall.Errno:
		if err := co.SendObj(hbi.Repr(int(fse))); err != nil {
			panic(err)
		}
		return
	default:
		panic(fsErr)
	}

	bufView := ((*[unsafe.Sizeof(*ce)]byte)(unsafe.Pointer(ce)))[0:unsafe.Sizeof(*ce)]
	if err := co.SendData(bufView); err != nil {
		panic(err)
	}
}

func (efs *exportedFileSystem) Rename(oldParent vfs.InodeID, oldName string, newParent vfs.InodeID, newName string) {
	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	fsErr := efs.icd.Rename(oldParent, oldName, newParent, newName)

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	switch fse := fsErr.(type) {
	case nil:
		if err := co.SendObj(`0`); err != nil {
			panic(err)
		}
	case syscall.Errno:
		if err := co.SendObj(hbi.Repr(int(fse))); err != nil {
			panic(err)
		}
		return
	default:
		panic(fsErr)
	}
}

func (efs *exportedFileSystem) RmDir(parent vfs.InodeID, name string) {
	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	fsErr := efs.icd.RmDir(parent, name)

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	switch fse := fsErr.(type) {
	case nil:
		if err := co.SendObj(`0`); err != nil {
			panic(err)
		}
	case syscall.Errno:
		if err := co.SendObj(hbi.Repr(int(fse))); err != nil {
			panic(err)
		}
		return
	default:
		panic(fsErr)
	}
}

func (efs *exportedFileSystem) Unlink(parent vfs.InodeID, name string) {
	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	fsErr := efs.icd.Unlink(parent, name)

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	switch fse := fsErr.(type) {
	case nil:
		if err := co.SendObj(`0`); err != nil {
			panic(err)
		}
	case syscall.Errno:
		if err := co.SendObj(hbi.Repr(int(fse))); err != nil {
			panic(err)
		}
		return
	default:
		panic(fsErr)
	}
}

func (efs *exportedFileSystem) OpenDir(inode vfs.InodeID) {
	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	handle, fsErr := efs.icd.OpenDir(inode)

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	switch fse := fsErr.(type) {
	case nil:
		if err := co.SendObj(`0`); err != nil {
			panic(err)
		}
	case syscall.Errno:
		if err := co.SendObj(hbi.Repr(int(fse))); err != nil {
			panic(err)
		}
		return
	default:
		panic(fsErr)
	}

	if err := co.SendObj(hbi.Repr(int(handle))); err != nil {
		panic(err)
	}
}

func (efs *exportedFileSystem) ReadDir(inode vfs.InodeID, handle int, offset int, bufSz int) {
	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	buf := efs.bufPool.Get(bufSz)
	defer efs.bufPool.Return(buf)

	bytesRead, fsErr := efs.icd.ReadDir(inode, handle, offset, buf)

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	switch fse := fsErr.(type) {
	case nil:
		if err := co.SendObj(`0`); err != nil {
			panic(err)
		}
	case syscall.Errno:
		if err := co.SendObj(hbi.Repr(int(fse))); err != nil {
			panic(err)
		}
		return
	default:
		panic(fsErr)
	}

	if err := co.SendObj(hbi.Repr(bytesRead)); err != nil {
		panic(err)
	}
	if bytesRead > 0 {
		if err := co.SendData(buf[:bytesRead]); err != nil {
			panic(err)
		}
	}
}

func (efs *exportedFileSystem) ReleaseDirHandle(handle int) {
	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	efs.icd.ReleaseDirHandle(handle)
}

func (efs *exportedFileSystem) OpenFile(inode vfs.InodeID, flags uint32) {
	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	handle, fsErr := efs.icd.OpenFile(inode, flags)

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	switch fse := fsErr.(type) {
	case nil:
		if err := co.SendObj(`0`); err != nil {
			panic(err)
		}
	case syscall.Errno:
		if err := co.SendObj(hbi.Repr(int(fse))); err != nil {
			panic(err)
		}
		return
	default:
		panic(fsErr)
	}

	if err := co.SendObj(hbi.Repr(int(handle))); err != nil {
		panic(err)
	}
}

func (efs *exportedFileSystem) ReadFile(inode vfs.InodeID, handle int, offset int64, bufSz int) {
	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	buf := efs.bufPool.Get(bufSz)
	defer efs.bufPool.Return(buf)

	bytesRead, fsErr := efs.icd.ReadFile(inode, handle, offset, buf)

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	eof := "false"
	if fsErr == io.EOF {
		eof = "true"
		fsErr = nil
	}

	switch fse := fsErr.(type) {
	case nil:
		if err := co.SendObj(`0`); err != nil {
			panic(err)
		}
	case syscall.Errno:
		if err := co.SendObj(hbi.Repr(int(fse))); err != nil {
			panic(err)
		}
		return
	default:
		panic(fsErr)
	}

	if err := co.SendObj(hbi.Repr(bytesRead)); err != nil {
		panic(err)
	}
	if err := co.SendObj(eof); err != nil {
		panic(err)
	}
	if bytesRead > 0 {
		if err := co.SendData(buf[:bytesRead]); err != nil {
			panic(err)
		}
	}
}
