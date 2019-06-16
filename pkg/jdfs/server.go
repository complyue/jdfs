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
	"github.com/complyue/jdfs/pkg/errors"
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

	var ce vfs.ChildInodeEntry
	fsErr := func() error {
		ici, ok := efs.icd.GetInode(0, parent)
		if !ok {
			return vfs.ENOENT
		}
		// the children map won't be modified after armed to ici, no sync needed to read it
		children := ici.children

		if children == nil || time.Now().Sub(ici.lastChildrenChecked) > vfs.DIR_CHILDREN_CACHE_TIME {
			// read dir contents from local fs, cache to children list
			parentM, childMs, outdatedPaths, err := readInodeDir(parent, ici.reachedThrough)
			if err != nil {
				return err
			}
			checkTime := time.Now()
			found := false
			children = make(map[string]vfs.InodeID, len(childMs))
			for i := range childMs {
				childM := &childMs[i]
				children[childM.name] = childM.inode
				if childM.name == name {
					if cici, ok := efs.icd.LoadInode(1, *childM, nil, nil, checkTime); ok {
						ce = vfs.ChildInodeEntry{
							Child:                cici.inode,
							Generation:           0,
							Attributes:           cici.attrs,
							AttributesExpiration: checkTime.Add(vfs.META_ATTRS_CACHE_TIME),
							EntryExpiration:      checkTime.Add(vfs.DIR_CHILDREN_CACHE_TIME),
						}
						found = true
					}
				}
			}
			if ici, ok = efs.icd.LoadInode(0, parentM, outdatedPaths, children, checkTime); !ok {
				return vfs.ENOENT
			}
			if found {
				return nil
			}
			return vfs.ENOENT
		} else {
			// use cached children map
			cInode, ok := children[name]
			if !ok {
				return vfs.ENOENT // no child with specified name
			}
			if cici, ok := efs.icd.GetInode(1, cInode); ok {
				// already in-core
				ce = vfs.ChildInodeEntry{
					Child:                cici.inode,
					Generation:           0,
					Attributes:           cici.attrs,
					AttributesExpiration: ici.lastChecked.Add(vfs.META_ATTRS_CACHE_TIME),
					EntryExpiration:      ici.lastChildrenChecked.Add(vfs.DIR_CHILDREN_CACHE_TIME),
				}
				return nil
			}
			// not yet in-core, consult local fs
			parentM, outdatedPaths, err := statInode(ici.inode, ici.reachedThrough)
			if err != nil {
				return err // failed stating parent dir
			}
			// update stat'ed parent meta data to in-core record
			if ici, ok = efs.icd.LoadInode(0, parentM, outdatedPaths, nil, time.Now()); !ok {
				return vfs.ENOENT // parent dir disappeared
			}
			childPath := parentM.jdfChildPath(name)
			if cFI, err := os.Lstat(childPath); err != nil {
				return err
			} else if cici, ok := efs.icd.LoadInode(1, fi2im("", cFI), nil, nil, time.Now()); ok {
				ce = vfs.ChildInodeEntry{
					Child:                cici.inode,
					Generation:           0,
					Attributes:           cici.attrs,
					AttributesExpiration: cici.lastChecked.Add(vfs.META_ATTRS_CACHE_TIME),
					EntryExpiration:      cici.lastChecked.Add(vfs.DIR_CHILDREN_CACHE_TIME),
				}
				return nil
			}
		}
		return vfs.ENOENT
	}()

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

	bufView := ((*[unsafe.Sizeof(ce)]byte)(unsafe.Pointer(&ce)))[0:unsafe.Sizeof(ce)]
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

	if ici, ok := efs.icd.GetInode(0, inode); !ok {
		fsErr = vfs.ENOENT
	} else if time.Now().Sub(ici.lastChecked) > vfs.META_ATTRS_CACHE_TIME {
		// refresh after cache time out
		if inoM, outdatedPaths, err := statInode(
			ici.inode, ici.reachedThrough,
		); err != nil {
			fsErr = err
		} else if ici, ok = efs.icd.LoadInode(0, inoM, outdatedPaths, nil, time.Now()); !ok {
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

	fsErr := func() error {
		if ici, ok := efs.icd.GetInode(0, inode); !ok {
			return vfs.ENOENT // no such inode
		} else if inoM, outdatedPaths, err := statInode(ici.inode, ici.reachedThrough); err != nil {
			return err // local fs error at jdfs
		} else {
			// update refreshed meta data to in-core inode record
			if ici, ok = efs.icd.LoadInode(0, inoM, outdatedPaths, nil, time.Now()); !ok {
				return vfs.ENOENT
			}

			// perform FUSE requested ops on local fs
			jdfPath := inoM.jdfPath()
			inoF, err := os.OpenFile(jdfPath, os.O_RDWR, 0)
			if err != nil {
				return err
			}
			defer inoF.Close()

			if chgSize {
				if err := inoF.Truncate(int64(sz)); err != nil {
					return err
				}
			}

			if chgMode {
				if err := inoF.Chmod(os.FileMode(mode)); err != nil {
					return err
				}
			}

			if chgMtime {
				if err := chftimes(inoF, mNsec); err != nil {
					return err
				}
			}

			// stat local fs again for new meta attrs
			if inoFI, err := os.Lstat(jdfPath); err != nil {
				return err // local fs error
			} else if ici, ok = efs.icd.LoadInode(0, fi2im("", inoFI), nil, nil, time.Now()); !ok {
				return vfs.ENOENT // inode disappeared
			} else {
				attrs = ici.attrs
				return nil
			}
		}
		return vfs.ENOENT
	}()

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

	var ce vfs.ChildInodeEntry

	fsErr := func() error {
		ici, ok := efs.icd.GetInode(0, parent)
		if !ok {
			return vfs.ENOENT
		}
		parentM, outdatedPaths, err := statInode(ici.inode, ici.reachedThrough)
		if err != nil {
			return err
		}
		if ici, ok = efs.icd.LoadInode(0, parentM, outdatedPaths, nil, time.Now()); !ok {
			return vfs.ENOENT
		}

		// perform requested FUSE op on local fs
		childPath := parentM.jdfChildPath(name)
		if err = os.Mkdir(childPath, os.FileMode(mode)); err != nil {
			return err
		}
		cFI, err := os.Lstat(childPath)
		if err != nil {
			return err
		}
		checkTime := time.Now()
		if cici, ok := efs.icd.LoadInode(1, fi2im("", cFI), nil, nil, checkTime); !ok {
			return vfs.ENOENT
		} else {
			efs.icd.InvalidateChildren(ici.inode, "", name)
			ce = vfs.ChildInodeEntry{
				Child:                cici.inode,
				Generation:           0,
				Attributes:           cici.attrs,
				AttributesExpiration: checkTime.Add(vfs.META_ATTRS_CACHE_TIME),
				EntryExpiration:      checkTime.Add(vfs.DIR_CHILDREN_CACHE_TIME),
			}
			return nil
		}
		return vfs.ENOENT
	}()

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

	bufView := ((*[unsafe.Sizeof(ce)]byte)(unsafe.Pointer(&ce)))[0:unsafe.Sizeof(ce)]
	if err := co.SendData(bufView); err != nil {
		panic(err)
	}
}

func (efs *exportedFileSystem) CreateFile(parent vfs.InodeID, name string, mode uint32) {
	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	var ce vfs.ChildInodeEntry
	handle, fsErr := func() (handle vfs.HandleID, err error) {
		var cF *os.File
		defer func() {
			if e := recover(); e != nil {
				err = errors.RichError(e)
			}
			if err != nil && cF != nil {
				cF.Close() // don't leak it on error
			}
		}()
		ici, ok := efs.icd.GetInode(0, parent)
		if !ok {
			err = vfs.ENOENT
			return
		}
		parentM, outdatedPaths, e := statInode(ici.inode, ici.reachedThrough)
		if e != nil {
			err = e
			return
		}
		if ici, ok = efs.icd.LoadInode(0, parentM, outdatedPaths, nil, time.Now()); !ok {
			err = e
			return
		}

		// perform requested FUSE op on local fs
		childPath := parentM.jdfChildPath(name)
		if cF, err = os.OpenFile(childPath,
			os.O_CREATE|os.O_EXCL, os.FileMode(mode),
		); err != nil {
			return
		}
		cFI, e := os.Lstat(childPath)
		if e != nil {
			err = e
			return
		}
		checkTime := time.Now()
		cici, ok := efs.icd.LoadInode(1, fi2im("", cFI), nil, nil, checkTime)
		if !ok {
			err = vfs.ENOENT
			return
		}
		efs.icd.InvalidateChildren(ici.inode, "", name)

		ce = vfs.ChildInodeEntry{
			Child:                cici.inode,
			Generation:           0,
			Attributes:           cici.attrs,
			AttributesExpiration: checkTime.Add(vfs.META_ATTRS_CACHE_TIME),
			EntryExpiration:      checkTime.Add(vfs.DIR_CHILDREN_CACHE_TIME),
		}

		if handle, err = efs.icd.CreateFileHandle(cici.inode, cF); err != nil {
			return
		}
		return
	}()

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

	bufView := ((*[unsafe.Sizeof(ce)]byte)(unsafe.Pointer(&ce)))[0:unsafe.Sizeof(ce)]
	if err := co.SendData(bufView); err != nil {
		panic(err)
	}
}

func (efs *exportedFileSystem) CreateSymlink(parent vfs.InodeID, name string, target string) {
	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	var ce vfs.ChildInodeEntry

	fsErr := func() error {
		ici, ok := efs.icd.GetInode(0, parent)
		if !ok {
			return vfs.ENOENT
		}
		parentM, outdatedPaths, err := statInode(ici.inode, ici.reachedThrough)
		if err != nil {
			return err
		}
		if ici, ok = efs.icd.LoadInode(0, parentM, outdatedPaths, nil, time.Now()); !ok {
			return err
		}

		// perform requested FUSE op on local fs
		childPath := parentM.jdfChildPath(name)
		if err = os.Symlink(target, childPath); err != nil {
			return err
		}
		cFI, err := os.Lstat(childPath)
		if err != nil {
			return err
		}
		checkTime := time.Now()
		if cici, ok := efs.icd.LoadInode(1, fi2im("", cFI), nil, nil, checkTime); !ok {
			return vfs.ENOENT
		} else {
			efs.icd.InvalidateChildren(ici.inode, "", name)
			ce = vfs.ChildInodeEntry{
				Child:                cici.inode,
				Generation:           0,
				Attributes:           cici.attrs,
				AttributesExpiration: checkTime.Add(vfs.META_ATTRS_CACHE_TIME),
				EntryExpiration:      checkTime.Add(vfs.DIR_CHILDREN_CACHE_TIME),
			}
			return nil
		}
		return vfs.ENOENT
	}()

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

	bufView := ((*[unsafe.Sizeof(ce)]byte)(unsafe.Pointer(&ce)))[0:unsafe.Sizeof(ce)]
	if err := co.SendData(bufView); err != nil {
		panic(err)
	}
}

func (efs *exportedFileSystem) CreateLink(parent vfs.InodeID, name string, target vfs.InodeID) {
	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	var ce vfs.ChildInodeEntry

	fsErr := func() error {
		ici, ok := efs.icd.GetInode(0, parent)
		if !ok {
			return vfs.ENOENT
		}
		parentM, outdatedPaths, err := statInode(ici.inode, ici.reachedThrough)
		if err != nil {
			return err
		}
		if ici, ok = efs.icd.LoadInode(0, parentM, outdatedPaths, nil, time.Now()); !ok {
			return err
		}

		iciTarget, ok := efs.icd.GetInode(0, target)
		targetM, outdatedPaths, err := statInode(iciTarget.inode, iciTarget.reachedThrough)
		if err != nil {
			return err
		}
		if iciTarget, ok = efs.icd.LoadInode(0, targetM, outdatedPaths, nil, time.Now()); !ok {
			return err
		}

		// perform requested FUSE op on local fs
		childPath := parentM.jdfChildPath(name)
		if err = os.Link(targetM.jdfPath(), childPath); err != nil {
			return err
		}
		cFI, err := os.Lstat(childPath)
		if err != nil {
			return err
		}
		checkTime := time.Now()
		if cici, ok := efs.icd.LoadInode(1, fi2im("", cFI), nil, nil, checkTime); !ok {
			return vfs.ENOENT
		} else {
			efs.icd.InvalidateChildren(ici.inode, "", name)
			ce = vfs.ChildInodeEntry{
				Child:                cici.inode,
				Generation:           0,
				Attributes:           cici.attrs,
				AttributesExpiration: checkTime.Add(vfs.META_ATTRS_CACHE_TIME),
				EntryExpiration:      checkTime.Add(vfs.DIR_CHILDREN_CACHE_TIME),
			}
			return nil
		}
		return vfs.ENOENT
	}()

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

	bufView := ((*[unsafe.Sizeof(ce)]byte)(unsafe.Pointer(&ce)))[0:unsafe.Sizeof(ce)]
	if err := co.SendData(bufView); err != nil {
		panic(err)
	}
}

func (efs *exportedFileSystem) Rename(oldParent vfs.InodeID, oldName string, newParent vfs.InodeID, newName string) {
	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	fsErr := func() error {
		iciOldParent, ok := efs.icd.GetInode(0, oldParent)
		if !ok {
			return vfs.ENOENT
		}
		oldParentM, outdatedPaths, err := statInode(iciOldParent.inode, iciOldParent.reachedThrough)
		if err != nil {
			return err
		}
		if iciOldParent, ok = efs.icd.LoadInode(0, oldParentM, outdatedPaths, nil, time.Now()); !ok {
			return err
		}

		iciNewParent, ok := efs.icd.GetInode(0, newParent)
		if !ok {
			return vfs.ENOENT
		}
		newParentM, outdatedPaths, err := statInode(iciNewParent.inode, iciNewParent.reachedThrough)
		if err != nil {
			return err
		}
		if iciNewParent, ok = efs.icd.LoadInode(0, newParentM, outdatedPaths, nil, time.Now()); !ok {
			return err
		}

		// perform requested FUSE op on local fs
		oldPath := oldParentM.jdfChildPath(oldName)
		newPath := newParentM.jdfChildPath(newName)
		if err = os.Rename(oldPath, newPath); err != nil {
			return err
		}
		if iciOldParent.inode == iciNewParent.inode {
			efs.icd.InvalidateChildren(iciNewParent.inode, oldName, newName)
		} else {
			efs.icd.InvalidateChildren(iciOldParent.inode, oldName, "")
			efs.icd.InvalidateChildren(iciNewParent.inode, "", newName)
		}
		return nil
	}()

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

	fsErr := func() error {
		ici, ok := efs.icd.GetInode(0, parent)
		if !ok {
			return vfs.ENOENT
		}
		parentM, outdatedPaths, err := statInode(ici.inode, ici.reachedThrough)
		if err != nil {
			return err
		}
		if ici, ok = efs.icd.LoadInode(0, parentM, outdatedPaths, nil, time.Now()); !ok {
			return err
		}

		// perform requested FUSE op on local fs
		childPath := parentM.jdfChildPath(name)
		if err = syscall.Rmdir(childPath); err != nil {
			return err
		}

		efs.icd.InvalidateChildren(ici.inode, name, "")

		return nil
	}()

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

	fsErr := func() error {
		ici, ok := efs.icd.GetInode(0, parent)
		if !ok {
			return vfs.ENOENT
		}
		parentM, outdatedPaths, err := statInode(ici.inode, ici.reachedThrough)
		if err != nil {
			return err
		}
		if ici, ok = efs.icd.LoadInode(0, parentM, outdatedPaths, nil, time.Now()); !ok {
			return err
		}

		// perform requested FUSE op on local fs
		childPath := parentM.jdfChildPath(name)
		if err = syscall.Unlink(childPath); err != nil {
			return err
		}

		efs.icd.InvalidateChildren(ici.inode, "", name)

		return nil
	}()

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

	var handle vfs.HandleID
	fsErr := func() error {
		ici, ok := efs.icd.GetInode(0, inode)
		if !ok {
			return vfs.ENOENT
		}
		parentM, childMs, outdatedPaths, err := readInodeDir(ici.inode, ici.reachedThrough)
		if err != nil {
			return err
		}
		checkTime := time.Now()

		var children map[string]vfs.InodeID
		var entries []vfs.DirEnt
		if len(childMs) > 0 {
			children = make(map[string]vfs.InodeID, len(childMs))
			entries = make([]vfs.DirEnt, 0, len(childMs))
		}
		for i := range childMs {
			childM := &childMs[i]

			children[childM.name] = childM.inode

			cMode := childM.attrs.Mode
			entType := vfs.DT_Unknown
			if cMode.IsDir() {
				entType = vfs.DT_Directory
			} else if cMode.IsRegular() {
				entType = vfs.DT_File
			} else if cMode&os.ModeSymlink != 0 {
				entType = vfs.DT_Link
			} else {
				continue // hide this strange inode to jdfc
			}
			entries = append(entries, vfs.DirEnt{
				Offset: vfs.DirOffset(len(entries) + 1),
				Inode:  childM.inode,
				Name:   childM.name,
				Type:   entType,
			})
		}
		if ici, ok = efs.icd.LoadInode(0, parentM, outdatedPaths, children, checkTime); !ok {
			return vfs.ENOENT
		}

		if handle, err = efs.icd.CreateDirHandle(inode, entries); err != nil {
			return err
		}

		return nil
	}()

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

	var bytesRead int
	var buf []byte
	icdh, fsErr := efs.icd.GetDirHandle(inode, handle)
	if fsErr == nil {
		buf = efs.bufPool.Get(bufSz)
		defer efs.bufPool.Return(buf)

		for i := offset; i < len(icdh.entries); i++ {
			n := vfs.WriteDirEnt(buf[bytesRead:], icdh.entries[i])
			if n <= 0 {
				break
			}
			bytesRead += n
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

	handle, fsErr := func() (handle vfs.HandleID, err error) {
		var oF *os.File
		defer func() {
			if e := recover(); e != nil {
				err = errors.RichError(e)
			}
			if err != nil && oF != nil {
				oF.Close() // don't leak it on error
			}
		}()
		ici, ok := efs.icd.GetInode(0, inode)
		if !ok {
			err = vfs.ENOENT
			return
		}
		inoM, outdatedPaths, e := statInode(ici.inode, ici.reachedThrough)
		if e != nil {
			err = e
			return
		}
		if ici, ok = efs.icd.LoadInode(0, inoM, outdatedPaths, nil, time.Now()); !ok {
			err = e
			return
		}

		jdfPath := inoM.jdfPath()
		if oF, err = os.OpenFile(jdfPath, int(flags), 0644); err != nil {
			return
		}

		if handle, err = efs.icd.CreateFileHandle(ici.inode, oF); err != nil {
			return
		}
		return
	}()

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

	var bytesRead int
	var buf []byte
	icfh, fsErr := efs.icd.GetFileHandle(inode, handle)
	if fsErr == nil {
		buf = efs.bufPool.Get(bufSz)
		defer efs.bufPool.Return(buf)

		bytesRead, fsErr = icfh.f.ReadAt(buf, offset)
	}

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

func (efs *exportedFileSystem) WriteFile(inode vfs.InodeID, handle int, offset int64, dataSz int) {
	co := efs.ho.Co()

	buf := efs.bufPool.Get(dataSz)
	defer efs.bufPool.Return(buf)
	if err := co.RecvData(buf); err != nil {
		panic(err)
	}

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	icfh, fsErr := efs.icd.GetFileHandle(inode, handle)
	if fsErr == nil {
		_, fsErr = icfh.f.WriteAt(buf, offset)
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
}

func (efs *exportedFileSystem) SyncFile(inode vfs.InodeID, handle int) {
	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	icfh, fsErr := efs.icd.GetFileHandle(inode, handle)
	if fsErr == nil {
		fsErr = icfh.f.Sync()
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
}

func (efs *exportedFileSystem) ReleaseFileHandle(handle int) {
	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	efs.icd.ReleaseFileHandle(handle)
}

func (efs *exportedFileSystem) ReadSymlink(inode vfs.InodeID) {
	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	target, fsErr := func() (target string, err error) {
		ici, ok := efs.icd.GetInode(0, inode)
		if !ok {
			err = vfs.ENOENT
			return
		}
		inoM, outdatedPaths, e := statInode(ici.inode, ici.reachedThrough)
		if e != nil {
			err = e
			return
		}
		if ici, ok = efs.icd.LoadInode(0, inoM, outdatedPaths, nil, time.Now()); !ok {
			err = e
			return
		}

		jdfPath := inoM.jdfPath()
		if target, err = os.Readlink(jdfPath); err != nil {
			return
		}

		return
	}()

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

	if err := co.SendObj(target); err != nil {
		panic(err)
	}
}
