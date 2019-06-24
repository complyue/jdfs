// Package jdfs defines implementation of the Just Data FileSystem server
package jdfs

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"
	"unsafe"

	"github.com/complyue/hbi"
	"github.com/complyue/hbi/interop"
	"github.com/complyue/jdfs/pkg/errors"
	"github.com/complyue/jdfs/pkg/vfs"

	"github.com/golang/glog"
)

func newServiceEnv(exportRoot string) *hbi.HostingEnv {
	// prepare the hosting environment to be reacting to jdfc
	he := hbi.NewHostingEnv()
	// expose names for interop
	interop.ExposeInterOpValues(he)
	// expose portable fs error constants
	he.ExposeValue("EOKAY", vfs.EOKAY)
	he.ExposeValue("EEXIST", vfs.EEXIST)
	he.ExposeValue("EINVAL", vfs.EINVAL)
	he.ExposeValue("EIO", vfs.EIO)
	he.ExposeValue("ENOENT", vfs.ENOENT)
	he.ExposeValue("ENOSYS", vfs.ENOSYS)
	he.ExposeValue("ENOTDIR", vfs.ENOTDIR)
	he.ExposeValue("ENOTEMPTY", vfs.ENOTEMPTY)
	he.ExposeValue("ERANGE", vfs.ERANGE)
	he.ExposeValue("ENOSPC", vfs.ENOSPC)
	he.ExposeValue("ENOATTR", vfs.ENOATTR)

	he.ExposeFunction("__hbi_init__", // callback on wire connected
		func(po *hbi.PostingEnd, ho *hbi.HostingEnd) {
			efs := &exportedFileSystem{
				exportRoot: exportRoot,

				po: po, ho: ho,
			}

			// expose efs as the reactor
			he.ExposeReactor(efs)
		})

	return he
}

type exportedFileSystem struct {
	// the root directory that this JDFS server is willing to export.
	//
	// a jdfc can mount jdfPath="/" for this root directory,
	// or it can mount any sub dir under this path.
	//
	// multiple local filesystems can be separately mounted under this path for different
	// jdfc to mount.
	//
	// todo for a JDFS mount to expose nested filesystems under its mounted root dir,
	// there're possibilities that inode numbers from different fs collide, maybe FUSE
	// generationNumber can be used to support that, or just don't support nested fs over
	// JDFS, as what we are doing now.
	exportRoot string

	// HBI posting/hosting ends
	po *hbi.PostingEnd
	ho *hbi.HostingEnd

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
		"OpenDir", "ReadDir", "ReleaseDirHandle", "OpenFile", "ReadFile", "WriteFile", "SyncFile",
		"ReleaseFileHandle", "ReadSymlink", "RemoveXattr", "GetXattr", "ListXattr", "SetXattr",
	}
}

func (efs *exportedFileSystem) Mount(readOnly bool, jdfsPath string) {
	efs.readOnly = readOnly

	var rootPath string
	if jdfsPath == "/" || jdfsPath == "" {
		rootPath = efs.exportRoot

	} else {
		rootPath = filepath.Join(efs.exportRoot, jdfsPath)
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
		jdfRootInode, jdfsUID, jdfsGID,
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

	var ce vfs.ChildInodeEntry
	fse := vfs.FsErr(func() error {
		ici, _, ok := efs.icd.GetInode(0, parent, 0)
		if !ok {
			return vfs.ENOENT
		}
		// the children map won't be modified after armed to ici, no sync needed to read it
		children := ici.children

		// cache dir entries at jdfs side with 10ms timeout.
		// note this has nothing to do with FUSE kernel caching.
		if children == nil || time.Now().Sub(ici.lastChildrenChecked) > 10*time.Millisecond {
			// read dir contents from local fs, cache to children list
			parentM, outdatedPaths, err := statInode(parent, ici.reachedThrough)
			if err != nil {
				return err
			}
			childMs, err := readInodeDir(parentM)
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
							Child:      cici.inode,
							Generation: 0,
							Attributes: cici.attrs,
						}
						found = true

						if glog.V(2) {
							glog.Infof("Resolved path [%s]:[%s]/[%s] to inode %d",
								jdfsRootPath, parentM.jdfPath, name, cici.inode)
						}
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
			if cici, _, ok := efs.icd.GetInode(1, cInode, 0); ok {
				// already in-core
				ce = vfs.ChildInodeEntry{
					Child:      cici.inode,
					Generation: 0,
					Attributes: cici.attrs,
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
			childPath := parentM.childPath(name)
			if cFI, err := os.Lstat(childPath); err != nil {
				return err
			} else if cici, ok := efs.icd.LoadInode(1, fi2im(childPath, cFI), nil, nil, time.Now()); ok {
				ce = vfs.ChildInodeEntry{
					Child:      cici.inode,
					Generation: 0,
					Attributes: cici.attrs,
				}
				return nil
			}
		}
		return vfs.ENOENT
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

	var attrs vfs.InodeAttributes

	fsErr := func() (err error) {
		var inoM iMeta
		ici, icfh, ok := efs.icd.GetInode(0, inode, 1)
		if !ok {
			err = vfs.ENOENT
			return
		}
		if icfh != nil {
			defer icfh.opc.Done()
			if inoM, err = statFileHandle(icfh); err != nil {
				return
			}
			if ici, ok = efs.icd.LoadInode(0, inoM, nil, nil, time.Now()); !ok {
				err = vfs.ENOENT
				return
			}
		} else {
			var outdatedPaths []string
			if inoM, outdatedPaths, err = statInode(
				ici.inode, ici.reachedThrough,
			); err != nil {
				return
			}
			if ici, ok = efs.icd.LoadInode(0, inoM, outdatedPaths, nil, time.Now()); !ok {
				err = vfs.ENOENT
				return
			}
		}
		attrs = ici.attrs
		return nil
	}()

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	fse := vfs.FsErr(fsErr)
	if err := co.SendObj(fse.Repr()); err != nil {
		panic(err)
	}
	if fse != 0 {
		return
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

	var attrs vfs.InodeAttributes

	fse := vfs.FsErr(func() (err error) {
		var outdatedPaths []string
		var inoF *os.File
		var writable bool
		var inoM iMeta
		ici, icfh, ok := efs.icd.GetInode(0, inode, 1)
		if !ok {
			return vfs.ENOENT // no such inode
		}
		if icfh != nil {
			defer icfh.opc.Done()
			if inoM, err = statFileHandle(icfh); err != nil {
				return
			}
			inoF, writable = icfh.f, icfh.writable
		} else {
			if inoM, outdatedPaths, err = statInode(ici.inode, ici.reachedThrough); err != nil {
				return
			}
		}
		jdfPath := inoM.jdfPath

		// perform FUSE requested ops on local fs

		if chgSize {
			if glog.V(2) {
				glog.Infof("SZ setting size of [%d] [%s]:[%s] to %d bytes", ici.inode,
					jdfsRootPath, jdfPath, sz)
			}
			if inoF == nil || !writable {
				if inoF, err = os.OpenFile(jdfPath, os.O_RDWR, 0); err != nil {
					return
				}
				defer inoF.Close()
			}
			if err = inoF.Truncate(int64(sz)); err != nil {
				return
			}
		}

		if chgMode {
			if glog.V(2) {
				glog.Infof("MOD setting mode of [%d] [%s]:[%s] to [%+v]", ici.inode,
					jdfsRootPath, jdfPath, os.FileMode(mode))
			}

			if inoF != nil {
				if err = inoF.Chmod(os.FileMode(mode)); err != nil {
					return
				}
			} else {
				if err = os.Chmod(jdfPath, os.FileMode(mode)); err != nil {
					return
				}
			}
		}

		if chgMtime {
			if glog.V(2) {
				glog.Infof("MTIM setting mtime of [%d] [%s]:[%s] to %v", ici.inode,
					jdfsRootPath, jdfPath, time.Unix(0, mNsec))
			}

			if inoF != nil && writable {
				if err = chftimes(inoF, jdfPath, mNsec); err != nil {
					return
				}
			} else {
				// TODO set mtime without opened
			}
		}

		// stat local fs again for new meta attrs
		if inoFI, e := os.Lstat(jdfPath); e != nil {
			err = e // local fs error
			return
		} else if ici, ok = efs.icd.LoadInode(0, fi2im(jdfPath, inoFI), outdatedPaths, nil, time.Now()); !ok {
			err = vfs.ENOENT // inode disappeared
			return
		}
		attrs = ici.attrs
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

	bufView := ((*[unsafe.Sizeof(attrs)]byte)(unsafe.Pointer(&attrs)))[0:unsafe.Sizeof(attrs)]
	if err := co.SendData(bufView); err != nil {
		panic(err)
	}
}

func (efs *exportedFileSystem) ForgetInode(inode vfs.InodeID, n int) {
	if inode == vfs.RootInodeID {
		glog.Warning("forgetting root inode ?!")
		return // never forget about root
	}

	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	refcnt := efs.icd.ForgetInode(inode, n)

	if glog.V(2) {
		glog.Infof("FORGET inode [%d] refcnt -%d => %d", inode, n, refcnt)
	}
}

func (efs *exportedFileSystem) MkDir(parent vfs.InodeID, name string, mode uint32) {
	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	var ce vfs.ChildInodeEntry

	fse := vfs.FsErr(func() error {
		ici, _, ok := efs.icd.GetInode(0, parent, 0)
		if !ok {
			return vfs.ENOENT
		}
		// parent can not have open file handle, always stat the local fs namespace for
		// a reachable path to parent dir.
		parentM, outdatedPaths, err := statInode(ici.inode, ici.reachedThrough)
		if err != nil {
			return err
		}
		if ici, ok = efs.icd.LoadInode(0, parentM, outdatedPaths, nil, time.Now()); !ok {
			return vfs.ENOENT
		}

		// perform requested FUSE op on local fs
		childPath := parentM.childPath(name)
		if err = os.Mkdir(childPath, os.FileMode(mode)); err != nil {
			return err
		}
		cFI, err := os.Lstat(childPath)
		if err != nil {
			return err
		}
		checkTime := time.Now()
		childM := fi2im(childPath, cFI)
		if glog.V(2) {
			glog.Infof("Made dir [%s]:[%s]/[%s] with mode [%+v] => [%+v]",
				jdfsRootPath, parentM.jdfPath, name,
				os.FileMode(mode), cFI.Mode())
		}
		if cici, ok := efs.icd.LoadInode(1, childM, nil, nil, checkTime); !ok {
			return vfs.ENOENT
		} else {
			efs.icd.InvalidateChildren(ici.inode, "", name)
			ce = vfs.ChildInodeEntry{
				Child:      cici.inode,
				Generation: 0,
				Attributes: cici.attrs,
			}
			return nil
		}
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
		parentPath := "<?!?>"
		var cF *os.File
		defer func() {
			if e := recover(); e != nil {
				err = errors.RichError(e)
			}
			if handle == 0 {
				if err == nil {
					panic(errors.New("bug?!"))
				}
				if cF != nil { // don't leak file object on error
					glog.Warningf("File [%s]:[%s]/[%s] created but no handle created for it.",
						jdfsRootPath, parentPath, name)
					cF.Close()
				}
			}
		}()
		ici, _, ok := efs.icd.GetInode(0, parent, 0)
		if !ok {
			err = vfs.ENOENT
			return
		}
		parentM, outdatedPaths, e := statInode(ici.inode, ici.reachedThrough)
		if e != nil {
			err = e
			return
		}
		parentPath = parentM.jdfPath
		if ici, ok = efs.icd.LoadInode(0, parentM, outdatedPaths, nil, time.Now()); !ok {
			err = e
			return
		}

		// perform requested FUSE op on local fs
		childPath := parentM.childPath(name)
		if cF, err = os.OpenFile(childPath,
			// TODO need to figure out how to tell whether end user has specified O_EXCL
			os.O_EXCL|os.O_CREATE|os.O_RDWR, os.FileMode(mode),
		); err != nil {
			return
		}
		cFI, e := os.Lstat(childPath)
		if e != nil {
			err = e
			return
		}
		checkTime := time.Now()
		childM := fi2im(childPath, cFI)
		cici, ok := efs.icd.LoadInode(1, childM, nil, nil, checkTime)
		if !ok {
			err = vfs.ENOENT
			return
		}
		efs.icd.InvalidateChildren(ici.inode, "", name)

		ce = vfs.ChildInodeEntry{
			Child:      cici.inode,
			Generation: 0,
			Attributes: cici.attrs,
		}

		if handle, err = efs.icd.CreateFileHandle(cici.inode, cF, true); err != nil {
			return
		}

		if glog.V(2) {
			glog.Infof("Created file [%d] [%s]:[%s]/[%s] with mode [%+v] => [%+v], as handle %d",
				cici.inode, jdfsRootPath, parentM.jdfPath, name,
				os.FileMode(mode), cFI.Mode(), handle)
		}

		return
	}()

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	fse := vfs.FsErr(fsErr)
	if err := co.SendObj(fse.Repr()); err != nil {
		panic(err)
	}
	if fse != 0 {
		return
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

	fse := vfs.FsErr(func() error {
		ici, _, ok := efs.icd.GetInode(0, parent, 0)
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
		childPath := parentM.childPath(name)
		if err = os.Symlink(target, childPath); err != nil {
			return err
		}
		cFI, err := os.Lstat(childPath)
		if err != nil {
			return err
		}
		checkTime := time.Now()
		childM := fi2im(childPath, cFI)

		if glog.V(2) {
			glog.Infof("Created symlink [%s]:[%s]/[%s] -> [%s] with mode [%+v]",
				jdfsRootPath, parentM.jdfPath, name,
				target, cFI.Mode())
		}

		if cici, ok := efs.icd.LoadInode(1, childM, nil, nil, checkTime); !ok {
			return vfs.ENOENT
		} else {
			efs.icd.InvalidateChildren(ici.inode, "", name)
			ce = vfs.ChildInodeEntry{
				Child:      cici.inode,
				Generation: 0,
				Attributes: cici.attrs,
			}
			return nil
		}
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

	fse := vfs.FsErr(func() error {
		ici, _, ok := efs.icd.GetInode(0, parent, 0)
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

		iciTarget, icfhTarget, ok := efs.icd.GetInode(0, target, 1)
		if !ok {
			return vfs.ENOENT
		}
		var targetM iMeta
		if icfhTarget != nil {
			defer icfhTarget.opc.Done()
			if targetM, err = statFileHandle(icfhTarget); err != nil {
				return err
			}
		} else {
			var outdatedPaths []string
			if targetM, outdatedPaths, err = statInode(iciTarget.inode, iciTarget.reachedThrough); err != nil {
				return err
			}
			if iciTarget, ok = efs.icd.LoadInode(0, targetM, outdatedPaths, nil, time.Now()); !ok {
				return vfs.ENOENT
			}
		}

		// perform requested FUSE op on local fs
		childPath := parentM.childPath(name)
		if err = os.Link(targetM.jdfPath, childPath); err != nil {
			return err
		}
		cFI, err := os.Lstat(childPath)
		if err != nil {
			return err
		}
		checkTime := time.Now()
		childM := fi2im(childPath, cFI)

		if glog.V(2) {
			glog.Infof("Created Link [%s]:[%s]/[%s] with mode [%+v]",
				jdfsRootPath, parentM.jdfPath, name,
				cFI.Mode())
		}

		if cici, ok := efs.icd.LoadInode(1, childM, nil, nil, checkTime); !ok {
			return vfs.ENOENT
		} else {
			efs.icd.InvalidateChildren(ici.inode, "", name)
			ce = vfs.ChildInodeEntry{
				Child:      cici.inode,
				Generation: 0,
				Attributes: cici.attrs,
			}
			return nil
		}
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

	fse := vfs.FsErr(func() error {
		iciOldParent, _, ok := efs.icd.GetInode(0, oldParent, 0)
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

		iciNewParent, _, ok := efs.icd.GetInode(0, newParent, 0)
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
		oldPath := oldParentM.childPath(oldName)
		newPath := newParentM.childPath(newName)
		if err = os.Rename(oldPath, newPath); err != nil {
			return err
		}

		// load meta data of renamed file to update its reachedThrough list
		newFI, err := os.Lstat(newPath)
		if err != nil {
			return err
		}
		checkTime := time.Now()
		newM := fi2im(newPath, newFI)
		_, ok = efs.icd.LoadInode(0, newM, []string{oldPath}, nil, checkTime)
		if !ok {
			return vfs.ENOENT
		}

		if glog.V(2) {
			glog.Infof("Renamed [%s]: [%s]/[%s] to [%s]/[%s]", jdfsRootPath,
				oldParentM.jdfPath, oldName, newParentM.jdfPath, newName)
		}

		if iciOldParent.inode == iciNewParent.inode {
			efs.icd.InvalidateChildren(iciNewParent.inode, oldName, newName)
		} else {
			efs.icd.InvalidateChildren(iciOldParent.inode, oldName, "")
			efs.icd.InvalidateChildren(iciNewParent.inode, "", newName)
		}
		return nil
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

func (efs *exportedFileSystem) RmDir(parent vfs.InodeID, name string) {
	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	fse := vfs.FsErr(func() error {
		ici, _, ok := efs.icd.GetInode(0, parent, 0)
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
		childPath := parentM.childPath(name)
		if err = syscall.Rmdir(childPath); err != nil {
			return err
		}

		if glog.V(2) {
			glog.Infof("Removed dir [%s]:[%s]/[%s]",
				jdfsRootPath, parentM.jdfPath, name)
		}

		efs.icd.InvalidateChildren(ici.inode, name, "")

		return nil
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

func (efs *exportedFileSystem) Unlink(parent vfs.InodeID, name string) {
	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	fse := vfs.FsErr(func() error {
		ici, _, ok := efs.icd.GetInode(0, parent, 0)
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
		childPath := parentM.childPath(name)
		if err = syscall.Unlink(childPath); err != nil {
			return err
		}

		if glog.V(2) {
			glog.Infof("Removed file [%s]:[%s]/[%s]",
				jdfsRootPath, parentM.jdfPath, name)
		}

		efs.icd.InvalidateChildren(ici.inode, "", name)

		return nil
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

func (efs *exportedFileSystem) OpenDir(inode vfs.InodeID) {
	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	var handle vfs.HandleID
	fse := vfs.FsErr(func() error {
		ici, _, ok := efs.icd.GetInode(0, inode, 0)
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
		childMs, err := readInodeDir(parentM)
		if err != nil {
			return err
		}
		checkTime := time.Now()

		if glog.V(2) {
			glog.Infof("Loaded %d entries for [%s]:[%s]", len(childMs), jdfsRootPath,
				parentM.jdfPath)
		}

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
				if glog.V(1) {
					glog.Infof("jdfs [%s]:[%s]/[%s] has mode [%v], not revealed to jdfc.",
						jdfsRootPath, parentM.jdfPath, childM.name, cMode)
				}
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

		i := offset
		for ; i < len(icdh.entries); i++ {
			n := vfs.WriteDirEnt(buf[bytesRead:], icdh.entries[i])
			if n <= 0 {
				break
			}
			bytesRead += n
		}

		if glog.V(2) {
			glog.Infof("Prepared %d (%d~%d) of %d entries for dir [%s]:[%s]", i-offset,
				offset, i, len(icdh.entries), jdfsRootPath, icdh.jdfPath)
		}
	}

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	fse := vfs.FsErr(fsErr)
	if err := co.SendObj(fse.Repr()); err != nil {
		panic(err)
	}
	if fse != 0 {
		return
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

	released := efs.icd.ReleaseDirHandle(handle)

	if glog.V(2) {
		glog.Infof("Released dir handle for [%s]:[%s]", jdfsRootPath, released.jdfPath)
	}
}

func (efs *exportedFileSystem) OpenFile(inode vfs.InodeID, writable, createIfNE bool) {
	co := efs.ho.Co()

	handle, fsErr := func() (handle vfs.HandleID, err error) {
		// do this before the underlying HBI wire released
		ici, icfh, ok := efs.icd.GetInode(0, inode, 1)
		if !ok {
			err = vfs.ENOENT
			return
		}

		if err := co.FinishRecv(); err != nil {
			panic(err)
		}

		var openFlags int
		if writable {
			openFlags = os.O_RDWR
		} else {
			openFlags = os.O_RDONLY
		}
		if createIfNE {
			openFlags |= os.O_CREATE
		}

		var oF *os.File
		defer func() {
			if e := recover(); e != nil {
				err = errors.RichError(e)
			}
			if err != nil && oF != nil {
				oF.Close() // don't leak it on error
			}
		}()
		if icfh != nil {
			defer icfh.opc.Done()
			jdfPath := icfh.f.Name()
			if writable && !icfh.writable {
				// can not dup a readonly handle for write, open a new one
				if oF, err = os.OpenFile(jdfPath, openFlags, 0644); err != nil {
					return
				}
			} else if !writable && icfh.writable {
				// todo should this be a problem ?
				glog.V(1).Infof("Reusing a writable file handle on [%d] [%s]:[%s] for readonly.",
					inode, jdfsRootPath, jdfPath)
			}
			if oF == nil {
				// fd can be dup'ed
				var fd int
				if fd, err = syscall.Dup(int(icfh.f.Fd())); err != nil {
					return
				}
				oF = os.NewFile(uintptr(fd), jdfPath)
			}
		} else {
			inoM, outdatedPaths, e := statInode(ici.inode, ici.reachedThrough)
			if e != nil {
				err = e
				return
			}
			if ici, ok = efs.icd.LoadInode(0, inoM, outdatedPaths, nil, time.Now()); !ok {
				err = e
				return
			}
			jdfPath := inoM.jdfPath
			if oF, err = os.OpenFile(jdfPath, openFlags, 0644); err != nil {
				return
			}
		}

		if handle, err = efs.icd.CreateFileHandle(inode, oF, writable); err != nil {
			return
		}

		if glog.V(2) {
			glog.Infof("Opened file [%d] [%s]:[%s] writable=%v, as handle %d",
				ici.inode, jdfsRootPath, oF.Name(), writable, handle)
		}

		return
	}()

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	fse := vfs.FsErr(fsErr)
	if err := co.SendObj(fse.Repr()); err != nil {
		panic(err)
	}
	if fse != 0 {
		return
	}

	if err := co.SendObj(hbi.Repr(int(handle))); err != nil {
		panic(err)
	}
}

func (efs *exportedFileSystem) ReadFile(inode vfs.InodeID, handle int, offset int64, bufSz int) {
	co := efs.ho.Co()

	// do this before the underlying HBI wire released
	icfh, fsErr := efs.icd.GetFileHandle(inode, handle, 1)

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	var bytesRead int
	var buf []byte
	if fsErr == nil {
		func() {
			defer icfh.opc.Done()

			buf = efs.bufPool.Get(bufSz)
			defer efs.bufPool.Return(buf)

			bytesRead, fsErr = icfh.f.ReadAt(buf, offset)

			if glog.V(2) {
				glog.Infof("Read %d bytes @%d from file [%d] [%s]:[%s] with handle %d", bytesRead, offset,
					icfh.inode, jdfsRootPath, icfh.f.Name(), handle)
			}
		}()
	}

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	eof := "false"
	if fsErr == io.EOF {
		eof = "true"
		fsErr = nil
	}

	fse := vfs.FsErr(fsErr)
	if err := co.SendObj(fse.Repr()); err != nil {
		panic(err)
	}
	if fse != 0 {
		return
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

	// do this before the underlying HBI wire released
	icfh, fsErr := efs.icd.GetFileHandle(inode, handle, 1)

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	if fsErr == nil {
		func() {
			defer icfh.opc.Done()

			bytesWritten := 0
			bytesWritten, fsErr = icfh.f.WriteAt(buf, offset)

			if glog.V(2) {
				glog.Infof("Written %d bytes @%d to file [%d] [%s]:[%s] with handle %d", bytesWritten, offset,
					icfh.inode, jdfsRootPath, icfh.f.Name(), handle)
			}
			if fsErr != nil {
				glog.Errorf("Error writing file [%d] [%s]:[%s] with handle %d - %+v",
					icfh.inode, jdfsRootPath, icfh.f.Name(), handle, fsErr)
			}
		}()
	}

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	fse := vfs.FsErr(fsErr)
	if err := co.SendObj(fse.Repr()); err != nil {
		panic(err)
	}
	if fse != 0 {
		return
	}
}

func (efs *exportedFileSystem) SyncFile(inode vfs.InodeID, handle int) {
	co := efs.ho.Co()

	// do this before the underlying HBI wire released
	icfh, fsErr := efs.icd.GetFileHandle(inode, handle, 1)

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	if fsErr == nil {
		func() {
			defer icfh.opc.Done()

			fsErr = icfh.f.Sync()

			if glog.V(2) {
				glog.Infof("Sync'ed file [%d] [%s]:[%s]", icfh.inode, jdfsRootPath, icfh.f.Name())
			}
		}()
	}

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	fse := vfs.FsErr(fsErr)
	if err := co.SendObj(fse.Repr()); err != nil {
		panic(err)
	}
	if fse != 0 {
		return
	}
}

func (efs *exportedFileSystem) ReleaseFileHandle(handle int) {
	co := efs.ho.Co()

	// do this before the underlying HBI wire released
	inode, f := efs.icd.ReleaseFileHandle(handle)

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	if f == nil {
		panic("no file pointer from released file handle ?!")
	}
	jdfPath := f.Name()
	if err := f.Close(); err != nil {
		glog.Errorf("Error on closing jdfs file [%s]:[%s] - %+v",
			jdfsRootPath, jdfPath, err)
	}

	if glog.V(2) {
		glog.Infof("REL file handle %d released for file [%d] [%s]:[%s]", handle, inode,
			jdfsRootPath, jdfPath)
	}
}

func (efs *exportedFileSystem) ReadSymlink(inode vfs.InodeID) {
	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	target, fsErr := func() (target string, err error) {
		ici, _, ok := efs.icd.GetInode(0, inode, 0)
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

		jdfPath := inoM.jdfPath
		if target, err = os.Readlink(jdfPath); err != nil {
			return
		}

		if glog.V(2) {
			glog.Infof("Resolved symlink [%s]: [%s] to [%s]", jdfsRootPath, jdfPath, target)
		}

		return
	}()

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	fse := vfs.FsErr(fsErr)
	if err := co.SendObj(fse.Repr()); err != nil {
		panic(err)
	}
	if fse != 0 {
		return
	}

	if err := co.SendObj(hbi.Repr(target)); err != nil {
		panic(err)
	}
}

func (efs *exportedFileSystem) RemoveXattr(inode vfs.InodeID, name string) {
	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	fsErr := func() (err error) {
		var jdfPath string
		ici, icfh, ok := efs.icd.GetInode(0, inode, 1)
		if !ok {
			err = vfs.ENOENT
			return
		}
		if icfh != nil {
			defer icfh.opc.Done()
			jdfPath = icfh.f.Name()
			if err = fremovexattr(int(icfh.f.Fd()), name); err != nil {
				return
			}
		} else {
			inoM, outdatedPaths, e := statInode(ici.inode, ici.reachedThrough)
			if e != nil {
				err = e
				return
			}
			if ici, ok = efs.icd.LoadInode(0, inoM, outdatedPaths, nil, time.Now()); !ok {
				err = e
				return
			}
			jdfPath = inoM.jdfPath
			if err = removexattr(jdfPath, name); err != nil {
				return
			}
		}

		if glog.V(2) {
			glog.Infof("Removed xattr [%s] from [%d] [%s]:[%s]", name, inode, jdfsRootPath, jdfPath)
		}

		return
	}()

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	fse := vfs.FsErr(fsErr)
	if err := co.SendObj(fse.Repr()); err != nil {
		panic(err)
	}
	if fse != 0 {
		return
	}
}

func (efs *exportedFileSystem) GetXattr(inode vfs.InodeID, name string, bufSz int) {
	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	var buf []byte
	if bufSz > 0 {
		buf = efs.bufPool.Get(bufSz)
		defer efs.bufPool.Return(buf)
	} else {
		buf = []byte{}
	}

	var bytesRead int
	var fsErr error
	func() {
		var jdfPath string
		ici, icfh, ok := efs.icd.GetInode(0, inode, 1)
		if !ok {
			fsErr = vfs.ENOENT
			return
		}
		if icfh != nil {
			defer icfh.opc.Done()
			jdfPath = icfh.f.Name()
			if bytesRead, fsErr = fgetxattr(int(icfh.f.Fd()), name, buf); fsErr != nil {
				return
			}
		} else {
			inoM, outdatedPaths, e := statInode(ici.inode, ici.reachedThrough)
			if e != nil {
				fsErr = e
				return
			}
			if ici, ok = efs.icd.LoadInode(0, inoM, outdatedPaths, nil, time.Now()); !ok {
				fsErr = e
				return
			}
			jdfPath = inoM.jdfPath
			if bytesRead, fsErr = getxattr(jdfPath, name, buf); fsErr != nil {
				return
			}
		}

		if glog.V(2) {
			glog.Infof("Read xattr [%s]=[%s] %d#>%d for file [%d] [%s]:[%s]", name, string(buf),
				bufSz, bytesRead, inode, jdfsRootPath, jdfPath)
		}
	}()

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	fse := vfs.FsErr(fsErr)
	if err := co.SendObj(fse.Repr()); err != nil {
		panic(err)
	}
	if fse != 0 && fse != vfs.ERANGE {
		return
	}

	if err := co.SendObj(hbi.Repr(bytesRead)); err != nil {
		panic(err)
	}

	if 0 < bytesRead && bytesRead <= bufSz {
		if err := co.SendData(buf[:bytesRead]); err != nil {
			panic(err)
		}
	}
}

func (efs *exportedFileSystem) ListXattr(inode vfs.InodeID, bufSz int) {
	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	var buf []byte
	if bufSz > 0 {
		buf = efs.bufPool.Get(bufSz)
		defer efs.bufPool.Return(buf)
	} else {
		buf = []byte{}
	}

	var bytesRead int
	var fsErr error
	func() {
		var jdfPath string
		ici, icfh, ok := efs.icd.GetInode(0, inode, 1)
		if !ok {
			fsErr = vfs.ENOENT
			return
		}
		if icfh != nil {
			defer icfh.opc.Done()
			jdfPath = icfh.f.Name()
			if bytesRead, fsErr = flistxattr(int(icfh.f.Fd()), buf); fsErr != nil && fsErr != syscall.ERANGE {
				return
			}
		} else {
			inoM, outdatedPaths, e := statInode(ici.inode, ici.reachedThrough)
			if e != nil {
				fsErr = e
				return
			}
			if ici, ok = efs.icd.LoadInode(0, inoM, outdatedPaths, nil, time.Now()); !ok {
				fsErr = e
				return
			}
			jdfPath = inoM.jdfPath
			if bytesRead, fsErr = listxattr(jdfPath, buf); fsErr != nil && fsErr != syscall.ERANGE {
				return
			}
		}

		if glog.V(2) {
			glog.Infof("Listed xattr %d=>%d bytes for file [%d] [%s]:[%s]",
				bufSz, bytesRead, inode, jdfsRootPath, jdfPath)
		}
	}()

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	fse := vfs.FsErr(fsErr)
	if err := co.SendObj(fse.Repr()); err != nil {
		panic(err)
	}
	if fse != 0 && fse != vfs.ERANGE {
		return
	}

	if err := co.SendObj(hbi.Repr(bytesRead)); err != nil {
		panic(err)
	}

	if 0 < bytesRead && bytesRead <= bufSz {
		if err := co.SendData(buf[:bytesRead]); err != nil {
			panic(err)
		}
	}
}

func (efs *exportedFileSystem) SetXattr(inode vfs.InodeID, name string, valSz int, flags int) {
	co := efs.ho.Co()

	buf := efs.bufPool.Get(valSz)
	defer efs.bufPool.Return(buf)

	if err := co.RecvData(buf); err != nil {
		panic(err)
	}

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	fsErr := func() (err error) {
		var jdfPath string
		ici, icfh, ok := efs.icd.GetInode(0, inode, 1)
		if !ok {
			err = vfs.ENOENT
			return
		}
		if icfh != nil {
			defer icfh.opc.Done()
			jdfPath = icfh.f.Name()
			if err = fsetxattr(int(icfh.f.Fd()), name, buf, flags); err != nil {
				return
			}
		} else {
			inoM, outdatedPaths, e := statInode(ici.inode, ici.reachedThrough)
			if e != nil {
				err = e
				return
			}
			if ici, ok = efs.icd.LoadInode(0, inoM, outdatedPaths, nil, time.Now()); !ok {
				err = e
				return
			}
			jdfPath = inoM.jdfPath
			if err = setxattr(jdfPath, name, buf, flags); err != nil {
				return
			}
		}

		if glog.V(2) {
			glog.Infof("Updated xattr [%s]=[%s] of [%d] [%s]:[%s]", name, buf,
				inode, jdfsRootPath, jdfPath)
		}

		return
	}()

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	fse := vfs.FsErr(fsErr)
	if err := co.SendObj(fse.Repr()); err != nil {
		panic(err)
	}
	if fse != 0 {
		return
	}
}
