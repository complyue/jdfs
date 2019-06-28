// Package jdfc defines the implementation of Just Data FileSystem client
package jdfc

import (
	"context"
	"fmt"
	"os"
	"sync"
	"syscall"
	"unsafe"

	"github.com/golang/glog"

	"github.com/complyue/jdfs/pkg/errors"
	"github.com/complyue/jdfs/pkg/fuse"
	"github.com/complyue/jdfs/pkg/vfs"

	"github.com/complyue/hbi"
	"github.com/complyue/hbi/interop"
)

// PrepareHostingEnv creates and prepares a hosting environment to be reacting to jdfs
func PrepareHostingEnv() *hbi.HostingEnv {
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

	return he
}

// MountJDFS mounts a remote JDFS filesystem directory (can be root path or sub
// directory under the exported root), to a local mountpoint, then serves
// fs operations over HBI connections between this jdfc and the jdfs, to be
// established by jdfsConnector.
func MountJDFS(
	jdfsConnector func(he *hbi.HostingEnv) (
		po *hbi.PostingEnd, ho *hbi.HostingEnd, err error,
	),
	jdfsPath string,
	mountpoint string,
	cfg *fuse.MountConfig,
) (err error) {
	var (
		po *hbi.PostingEnd
		ho *hbi.HostingEnd
	)
	defer func() {
		if e := recover(); e != nil {
			err = errors.RichError(e)
		}
		if err != nil {
			glog.Errorf("Unexpected jdfc error: %+v", err)
		}
		if po != nil && !po.Disconnected() {
			if err != nil {
				po.Disconnect(fmt.Sprintf("Unexpected jdfc error: %+v", err), true)
			} else {
				po.Close()
			}
		}
	}()

	he := PrepareHostingEnv()

	fs := &fileSystem{
		readOnly: cfg.ReadOnly,
		jdfsPath: jdfsPath,

		jdfcUID: uint32(os.Geteuid()), jdfcGID: uint32(os.Getegid()),
	}

	// expose fs as the reactor
	he.ExposeReactor(fs)

	dialHBI := func() error {
		po, ho, err = jdfsConnector(he)
		if err != nil {
			return err
		}

		fs.connReset(po, ho)

		return nil
	}

	he.ExposeFunction("__hbi_cleanup__", func(discReason string) {
		// terminate jdfc (the FUSE user process), this leaves the mountpoint denying all
		// services. this is actually better than unmounting it, as naive programs may
		// think all files have been deleted due to the unmount, or even
		// start writing new files under paths of the mountpoint (which is not JDFS anymore).
		//
		// next run of jdfc for the same mountpoint will try unmounting immediately
		// before the new mounting attempt, if broken FUSE mount detected. that's not
		// perfect yet, but opens much smaller window of time for naive programs working
		// on the JDFS mount to make mistakes.

		if len(discReason) > 0 {
			fmt.Printf("jdfs disconnected due to: %s", discReason)
			os.Exit(6)
		}
		os.Exit(0)

		// todo auto reconnect jdfs. but need to figure out the way to tell FUSE kernel
		//      to invalidate all cache and handles in this case ?
	})

	if err = dialHBI(); err != nil {
		return err
	}

	mfs, fuseConn, err := fuse.Mount(mountpoint, &fileSystemServer{fs: fs}, cfg)
	if err != nil {
		return err
	}

	if p := mfs.Protocol(); !p.HasInvalidate() {
		err = errors.Errorf("FUSE kernel version %v not supported", p)
		return
	}
	func() {
		fs.mu.Lock()
		defer fs.mu.Unlock()

		fs.fuseConn = fuseConn
	}()

	fmt.Fprintf(os.Stderr, "JDFS client %d mounted [%s] on [%s]\n",
		os.Getpid(), cfg.FSName, mountpoint)

	if err = mfs.Join(context.Background()); err != nil {
		return err
	}

	return nil
}

type fileSystem struct {
	readOnly bool
	jdfsPath string

	jdfcUID, jdfcGID uint32

	mu sync.Mutex

	fuseConn *fuse.Connection

	po *hbi.PostingEnd
	ho *hbi.HostingEnd

	jdfsUID, jdfsGID uint32
}

func (fs *fileSystem) NamesToExpose() []string {
	return []string{
		"InvalidateNode",
		"InvalidateEntry",
	}
}

func (fs *fileSystem) mapOwner(attrs *vfs.InodeAttributes) {
	if attrs.Uid == fs.jdfsUID {
		attrs.Uid = fs.jdfcUID
	}
	if attrs.Gid == fs.jdfsGID {
		attrs.Gid = fs.jdfcGID
	}
}

func (fs *fileSystem) connReset(
	po *hbi.PostingEnd, ho *hbi.HostingEnd,
) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	fs.po, fs.ho = po, ho
	if err := func() (err error) {
		defer func() {
			if e := recover(); e != nil {
				err = errors.RichError(e)
			}
		}()

		// initiate mount
		var co *hbi.PoCo
		co, err = po.NewCo()
		if err != nil {
			return
		}
		defer co.Close()
		if err = co.SendCode(fmt.Sprintf(`
Mount(%#v, %#v)
`, fs.readOnly, fs.jdfsPath)); err != nil {
			return
		}
		if err = co.StartRecv(); err != nil {
			return
		}
		mountResult, err := co.RecvObj()
		if err != nil {
			return err
		}
		mountedFields := mountResult.(hbi.LitListType)
		fs.jdfsUID = uint32(mountedFields[1].(hbi.LitIntType))
		fs.jdfsGID = uint32(mountedFields[2].(hbi.LitIntType))

		return
	}(); err != nil {
		fs.po, fs.ho = nil, nil
		glog.Errorf("Error comm with jdfs: %+v", err)
		if !po.Disconnected() {
			po.Disconnect(fmt.Sprintf("server mount failed: %v", err), false)
		}
		os.Exit(7) // fail hard
	}
}

func (fs *fileSystem) InvalidateNode(
	inode vfs.InodeID, offset, size int64,
) {
	if err := fs.fuseConn.InvalidateNode(inode, offset, size); err != nil && err != vfs.ENOENT {
		glog.Fatalf("Unexpected fuse kernel error on inode invalidation [%T] - %+v", err, err)
	}
}

func (fs *fileSystem) InvalidateEntry(
	parent vfs.InodeID, name string,
) {
	if err := fs.fuseConn.InvalidateEntry(parent, name); err != nil && err != vfs.ENOENT {
		glog.Fatalf("Unexpected fuse kernel error on entry invalidation [%T] - %+v", err, err)
	}
}

func (fs *fileSystem) StatFS(
	ctx context.Context,
	op *vfs.StatFSOp) (err error) {
	co, err := fs.po.NewCo()
	if err != nil {
		panic(err)
	}
	defer co.Close()
	if err = co.SendCode(`StatFS()`); err != nil {
		panic(err)
	}
	if err = co.StartRecv(); err != nil {
		panic(err)
	}
	bufView := ((*[unsafe.Sizeof(*op)]byte)(unsafe.Pointer(op)))[0:unsafe.Sizeof(*op)]
	if err = co.RecvData(bufView); err != nil {
		panic(err)
	}
	return
}

func (fs *fileSystem) LookUpInode(
	ctx context.Context,
	op *vfs.LookUpInodeOp) (err error) {
	co, err := fs.po.NewCo()
	if err != nil {
		panic(err)
	}
	defer co.Close()
	if err = co.SendCode(fmt.Sprintf(`
LookUpInode(%#v, %#v)
`, op.Parent, op.Name)); err != nil {
		panic(err)
	}

	if err = co.StartRecv(); err != nil {
		return
	}

	if fsErr, err := co.RecvObj(); err != nil {
		panic(err)
	} else if fse, ok := fsErr.(vfs.FsError); !ok {
		panic(errors.Errorf("Unexpected fs error from jdfs with type [%T] - %+v", fsErr, fsErr))
	} else if fse != 0 {
		return syscall.Errno(fse)
	}

	bufView := ((*[unsafe.Sizeof(op.Entry)]byte)(unsafe.Pointer(&op.Entry)))[:unsafe.Sizeof(op.Entry)]
	if err = co.RecvData(bufView); err != nil {
		panic(err)
	}

	fs.mapOwner(&op.Entry.Attributes)

	return
}

func (fs *fileSystem) GetInodeAttributes(
	ctx context.Context,
	op *vfs.GetInodeAttributesOp) (err error) {
	co, err := fs.po.NewCo()
	if err != nil {
		panic(err)
	}
	defer co.Close()

	if err = co.SendCode(fmt.Sprintf(`
GetInodeAttributes(%#v)
`, op.Inode)); err != nil {
		panic(err)
	}

	if err = co.StartRecv(); err != nil {
		return
	}

	if fsErr, err := co.RecvObj(); err != nil {
		panic(err)
	} else if fse, ok := fsErr.(vfs.FsError); !ok {
		panic(errors.Errorf("Unexpected fs error from jdfs with type [%T] - %+v", fsErr, fsErr))
	} else if fse != 0 {
		return syscall.Errno(fse)
	}

	bufView := ((*[unsafe.Sizeof(op.Attributes)]byte)(unsafe.Pointer(&op.Attributes)))[:unsafe.Sizeof(op.Attributes)]
	if err = co.RecvData(bufView); err != nil {
		panic(err)
	}

	fs.mapOwner(&op.Attributes)

	return
}

func (fs *fileSystem) SetInodeAttributes(
	ctx context.Context,
	op *vfs.SetInodeAttributesOp) (err error) {
	co, err := fs.po.NewCo()
	if err != nil {
		panic(err)
	}
	defer co.Close()

	// intentionally avoid atime update
	var (
		chgSizeTo      uint64
		chgModeTo      uint32
		chgMtimeToNsec int64
	)
	if op.Size != nil {
		chgSizeTo = *op.Size
	}
	if op.Mode != nil {
		chgModeTo = uint32(*op.Mode)
	}
	if op.Mtime != nil {
		chgMtimeToNsec = *op.Mtime
	}

	if err = co.SendCode(fmt.Sprintf(`
SetInodeAttributes(%#v,%#v, %#v, %#v,%#v, %#v, %#v)
`, op.Inode,
		op.Size != nil, op.Mode != nil, op.Mtime != nil,
		chgSizeTo, chgModeTo, chgMtimeToNsec,
	)); err != nil {
		panic(err)
	}

	if err = co.StartRecv(); err != nil {
		panic(err)
	}

	if fsErr, err := co.RecvObj(); err != nil {
		panic(err)
	} else if fse, ok := fsErr.(vfs.FsError); !ok {
		panic(errors.Errorf("Unexpected fs error from jdfs with type [%T] - %+v", fsErr, fsErr))
	} else if fse != 0 {
		return syscall.Errno(fse)
	}

	bufView := ((*[unsafe.Sizeof(op.Attributes)]byte)(unsafe.Pointer(&op.Attributes)))[:unsafe.Sizeof(op.Attributes)]
	if err = co.RecvData(bufView); err != nil {
		panic(err)
	}

	fs.mapOwner(&op.Attributes)

	return
}

func (fs *fileSystem) ForgetInode(
	ctx context.Context,
	op *vfs.ForgetInodeOp) (err error) {
	co, err := fs.po.NewCo()
	if err != nil {
		panic(err)
	}
	defer co.Close()

	if err = co.SendCode(fmt.Sprintf(`
ForgetInode(%#v, %#v)
`, op.Inode, op.N)); err != nil {
		panic(err)
	}

	return
}

func (fs *fileSystem) MkDir(
	ctx context.Context,
	op *vfs.MkDirOp) (err error) {
	co, err := fs.po.NewCo()
	if err != nil {
		panic(err)
	}
	defer co.Close()

	if err = co.SendCode(fmt.Sprintf(`
MkDir(%#v, %#v, %#v)
`, op.Parent, op.Name, uint32(op.Mode))); err != nil {
		panic(err)
	}

	if err = co.StartRecv(); err != nil {
		panic(err)
	}

	if fsErr, err := co.RecvObj(); err != nil {
		panic(err)
	} else if fse, ok := fsErr.(vfs.FsError); !ok {
		panic(errors.Errorf("Unexpected fs error from jdfs with type [%T] - %+v", fsErr, fsErr))
	} else if fse != 0 {
		return syscall.Errno(fse)
	}

	bufView := ((*[unsafe.Sizeof(op.Entry)]byte)(unsafe.Pointer(&op.Entry)))[:unsafe.Sizeof(op.Entry)]
	if err = co.RecvData(bufView); err != nil {
		panic(err)
	}

	fs.mapOwner(&op.Entry.Attributes)

	return
}

func (fs *fileSystem) MkNode(
	ctx context.Context,
	op *vfs.MkNodeOp) (err error) {
	err = vfs.ENOSYS
	return
}

func (fs *fileSystem) CreateFile(
	ctx context.Context,
	op *vfs.CreateFileOp) (err error) {
	co, err := fs.po.NewCo()
	if err != nil {
		panic(err)
	}
	defer co.Close()

	if err = co.SendCode(fmt.Sprintf(`
CreateFile(%#v, %#v, %#v)
`, op.Parent, op.Name, uint32(op.Mode))); err != nil {
		panic(err)
	}

	if err = co.StartRecv(); err != nil {
		panic(err)
	}

	if fsErr, err := co.RecvObj(); err != nil {
		panic(err)
	} else if fse, ok := fsErr.(vfs.FsError); !ok {
		panic(errors.Errorf("Unexpected fs error from jdfs with type [%T] - %+v", fsErr, fsErr))
	} else if fse != 0 {
		return syscall.Errno(fse)
	}

	handle, err := co.RecvObj()
	if err != nil {
		panic(err)
	}
	if handle, ok := handle.(hbi.LitIntType); !ok {
		panic(errors.Errorf("unexpected handle type [%T] of handle value [%v]", handle, handle))
	} else {
		op.Handle = vfs.HandleID(handle)
	}

	bufView := ((*[unsafe.Sizeof(op.Entry)]byte)(unsafe.Pointer(&op.Entry)))[:unsafe.Sizeof(op.Entry)]
	if err = co.RecvData(bufView); err != nil {
		panic(err)
	}

	fs.mapOwner(&op.Entry.Attributes)

	return
}

func (fs *fileSystem) CreateSymlink(
	ctx context.Context,
	op *vfs.CreateSymlinkOp) (err error) {
	co, err := fs.po.NewCo()
	if err != nil {
		panic(err)
	}
	defer co.Close()

	if err = co.SendCode(fmt.Sprintf(`
CreateSymlink(%#v, %#v, %#v)
`, op.Parent, op.Name, op.Target)); err != nil {
		panic(err)
	}

	if err = co.StartRecv(); err != nil {
		panic(err)
	}

	if fsErr, err := co.RecvObj(); err != nil {
		panic(err)
	} else if fse, ok := fsErr.(vfs.FsError); !ok {
		panic(errors.Errorf("Unexpected fs error from jdfs with type [%T] - %+v", fsErr, fsErr))
	} else if fse != 0 {
		return syscall.Errno(fse)
	}

	bufView := ((*[unsafe.Sizeof(op.Entry)]byte)(unsafe.Pointer(&op.Entry)))[:unsafe.Sizeof(op.Entry)]
	if err = co.RecvData(bufView); err != nil {
		panic(err)
	}

	fs.mapOwner(&op.Entry.Attributes)

	return
}

func (fs *fileSystem) CreateLink(
	ctx context.Context,
	op *vfs.CreateLinkOp) (err error) {
	co, err := fs.po.NewCo()
	if err != nil {
		panic(err)
	}
	defer co.Close()

	if err = co.SendCode(fmt.Sprintf(`
CreateLink(%#v, %#v, %#v)
`, op.Parent, op.Name, op.Target)); err != nil {
		panic(err)
	}

	if err = co.StartRecv(); err != nil {
		panic(err)
	}

	if fsErr, err := co.RecvObj(); err != nil {
		panic(err)
	} else if fse, ok := fsErr.(vfs.FsError); !ok {
		panic(errors.Errorf("Unexpected fs error from jdfs with type [%T] - %+v", fsErr, fsErr))
	} else if fse != 0 {
		return syscall.Errno(fse)
	}

	bufView := ((*[unsafe.Sizeof(op.Entry)]byte)(unsafe.Pointer(&op.Entry)))[:unsafe.Sizeof(op.Entry)]
	if err = co.RecvData(bufView); err != nil {
		panic(err)
	}

	fs.mapOwner(&op.Entry.Attributes)

	return
}

func (fs *fileSystem) Rename(
	ctx context.Context,
	op *vfs.RenameOp) (err error) {
	co, err := fs.po.NewCo()
	if err != nil {
		panic(err)
	}
	defer co.Close()

	if err = co.SendCode(fmt.Sprintf(`
Rename(%#v, %#v, %#v, %#v)
`, op.OldParent, op.OldName, op.NewParent, op.NewName)); err != nil {
		panic(err)
	}

	if err = co.StartRecv(); err != nil {
		panic(err)
	}

	if fsErr, err := co.RecvObj(); err != nil {
		panic(err)
	} else if fse, ok := fsErr.(vfs.FsError); !ok {
		panic(errors.Errorf("Unexpected fs error from jdfs with type [%T] - %+v", fsErr, fsErr))
	} else if fse != 0 {
		return syscall.Errno(fse)
	}

	return
}

func (fs *fileSystem) RmDir(
	ctx context.Context,
	op *vfs.RmDirOp) (err error) {
	co, err := fs.po.NewCo()
	if err != nil {
		panic(err)
	}
	defer co.Close()

	if err = co.SendCode(fmt.Sprintf(`
RmDir(%#v, %#v)
`, op.Parent, op.Name)); err != nil {
		panic(err)
	}

	if err = co.StartRecv(); err != nil {
		panic(err)
	}

	if fsErr, err := co.RecvObj(); err != nil {
		panic(err)
	} else if fse, ok := fsErr.(vfs.FsError); !ok {
		panic(errors.Errorf("Unexpected fs error from jdfs with type [%T] - %+v", fsErr, fsErr))
	} else if fse != 0 {
		return syscall.Errno(fse)
	}

	return
}

func (fs *fileSystem) Unlink(
	ctx context.Context,
	op *vfs.UnlinkOp) (err error) {
	co, err := fs.po.NewCo()
	if err != nil {
		panic(err)
	}
	defer co.Close()

	if err = co.SendCode(fmt.Sprintf(`
Unlink(%#v, %#v)
`, op.Parent, op.Name)); err != nil {
		panic(err)
	}

	if err = co.StartRecv(); err != nil {
		panic(err)
	}

	if fsErr, err := co.RecvObj(); err != nil {
		panic(err)
	} else if fse, ok := fsErr.(vfs.FsError); !ok {
		panic(errors.Errorf("Unexpected fs error from jdfs with type [%T] - %+v", fsErr, fsErr))
	} else if fse != 0 {
		return syscall.Errno(fse)
	}

	return
}

func (fs *fileSystem) OpenDir(
	ctx context.Context,
	op *vfs.OpenDirOp) (err error) {
	co, err := fs.po.NewCo()
	if err != nil {
		panic(err)
	}
	defer co.Close()

	if err = co.SendCode(fmt.Sprintf(`
OpenDir(%#v)
`, op.Inode)); err != nil {
		panic(err)
	}

	if err = co.StartRecv(); err != nil {
		panic(err)
	}

	if fsErr, err := co.RecvObj(); err != nil {
		panic(err)
	} else if fse, ok := fsErr.(vfs.FsError); !ok {
		panic(errors.Errorf("Unexpected fs error from jdfs with type [%T] - %+v", fsErr, fsErr))
	} else if fse != 0 {
		return syscall.Errno(fse)
	}

	handle, err := co.RecvObj()
	if err != nil {
		panic(err)
	}
	if handle, ok := handle.(hbi.LitIntType); !ok {
		panic(errors.Errorf("unexpected handle type [%T] of handle value [%v]", handle, handle))
	} else {
		op.Handle = vfs.HandleID(handle)
	}

	return
}

func (fs *fileSystem) ReadDir(
	ctx context.Context,
	op *vfs.ReadDirOp) (err error) {
	co, err := fs.po.NewCo()
	if err != nil {
		panic(err)
	}
	defer co.Close()

	if err = co.SendCode(fmt.Sprintf(`
ReadDir(%#v, %#v, %#v, %#v)
`, op.Inode, op.Handle, op.Offset, len(op.Dst))); err != nil {
		panic(err)
	}

	if err = co.StartRecv(); err != nil {
		panic(err)
	}

	if fsErr, err := co.RecvObj(); err != nil {
		panic(err)
	} else if fse, ok := fsErr.(vfs.FsError); !ok {
		panic(errors.Errorf("Unexpected fs error from jdfs with type [%T] - %+v", fsErr, fsErr))
	} else if fse != 0 {
		return syscall.Errno(fse)
	}

	bytesRead, err := co.RecvObj()
	if err != nil {
		panic(err)
	}
	if bytesRead, ok := bytesRead.(hbi.LitIntType); !ok {
		panic(errors.Errorf("unexpected bytesRead type [%T] of bytesRead value [%v]", bytesRead, bytesRead))
	} else {
		op.BytesRead = int(bytesRead)
	}
	if op.BytesRead > 0 {
		if err = co.RecvData(op.Dst[:op.BytesRead]); err != nil {
			panic(err)
		}
	}

	return
}

func (fs *fileSystem) ReleaseDirHandle(
	ctx context.Context,
	op *vfs.ReleaseDirHandleOp) (err error) {
	co, err := fs.po.NewCo()
	if err != nil {
		panic(err)
	}
	defer co.Close()

	if err = co.SendCode(fmt.Sprintf(`
ReleaseDirHandle(%#v)
`, op.Handle)); err != nil {
		panic(err)
	}

	return
}

func (fs *fileSystem) OpenFile(
	ctx context.Context,
	op *vfs.OpenFileOp) (err error) {
	co, err := fs.po.NewCo()
	if err != nil {
		panic(err)
	}
	defer co.Close()

	// always favor kernel page cache over direct io with JDFS
	// TODO check page cache invalidation properly implemented
	op.KeepPageCache = true
	op.UseDirectIO = false

	writable := (int(op.Flags) & (os.O_RDWR | os.O_WRONLY | os.O_APPEND)) != 0
	createIfNE := (int(op.Flags) | os.O_CREATE) != 0
	if err = co.SendCode(fmt.Sprintf(`
OpenFile(%#v, %#v, %#v)
`, op.Inode, writable, createIfNE)); err != nil {
		panic(err)
	}

	if err = co.StartRecv(); err != nil {
		panic(err)
	}

	if fsErr, err := co.RecvObj(); err != nil {
		panic(err)
	} else if fse, ok := fsErr.(vfs.FsError); !ok {
		panic(errors.Errorf("Unexpected fs error from jdfs with type [%T] - %+v", fsErr, fsErr))
	} else if fse != 0 {
		return syscall.Errno(fse)
	}

	handle, err := co.RecvObj()
	if err != nil {
		panic(err)
	}
	if handle, ok := handle.(hbi.LitIntType); !ok {
		panic(errors.Errorf("unexpected handle type [%T] of handle value [%v]", handle, handle))
	} else {
		op.Handle = vfs.HandleID(handle)
	}

	return
}

func (fs *fileSystem) ReadFile(
	ctx context.Context,
	op *vfs.ReadFileOp) (err error) {
	co, err := fs.po.NewCo()
	if err != nil {
		panic(err)
	}
	defer co.Close()

	if err = co.SendCode(fmt.Sprintf(`
ReadFile(%#v, %#v, %#v, %#v)
`, op.Inode, op.Handle, op.Offset, len(op.Dst))); err != nil {
		panic(err)
	}

	if err = co.StartRecv(); err != nil {
		panic(err)
	}

	if fsErr, err := co.RecvObj(); err != nil {
		panic(err)
	} else if fse, ok := fsErr.(vfs.FsError); !ok {
		panic(errors.Errorf("Unexpected fs error from jdfs with type [%T] - %+v", fsErr, fsErr))
	} else if fse != 0 {
		return syscall.Errno(fse)
	}

	bytesRead, err := co.RecvObj()
	if err != nil {
		panic(err)
	}
	if bytesRead, ok := bytesRead.(hbi.LitIntType); !ok {
		panic(errors.Errorf("unexpected bytesRead type [%T] of bytesRead value [%v]", bytesRead, bytesRead))
	} else {
		op.BytesRead = int(bytesRead)
	}

	eof, err := co.RecvObj()
	if err != nil {
		panic(err)
	}

	if op.BytesRead > 0 {
		if err = co.RecvData(op.Dst[:op.BytesRead]); err != nil {
			panic(err)
		}
	}

	if eof.(bool) {
		// return EOF only in directio mode
		// TODO figure out whether we'd support directio.
		// return io.EOF
	}

	return
}

func (fs *fileSystem) WriteFile(
	ctx context.Context,
	op *vfs.WriteFileOp) (err error) {
	co, err := fs.po.NewCo()
	if err != nil {
		panic(err)
	}
	defer co.Close()

	if err = co.SendCode(fmt.Sprintf(`
WriteFile(%#v, %#v, %#v, %#v)
`, op.Inode, op.Handle, op.Offset, len(op.Data))); err != nil {
		panic(err)
	}
	if err = co.SendData(op.Data); err != nil {
		panic(err)
	}

	if err = co.StartRecv(); err != nil {
		panic(err)
	}

	if fsErr, err := co.RecvObj(); err != nil {
		panic(err)
	} else if fse, ok := fsErr.(vfs.FsError); !ok {
		panic(errors.Errorf("Unexpected fs error from jdfs with type [%T] - %+v", fsErr, fsErr))
	} else if fse != 0 {
		return syscall.Errno(fse)
	}

	return
}

func (fs *fileSystem) SyncFile(
	ctx context.Context,
	op *vfs.SyncFileOp) (err error) {
	co, err := fs.po.NewCo()
	if err != nil {
		panic(err)
	}
	defer co.Close()

	if err = co.SendCode(fmt.Sprintf(`
SyncFile(%#v, %#v)
`, op.Inode, op.Handle)); err != nil {
		panic(err)
	}

	if err = co.StartRecv(); err != nil {
		panic(err)
	}

	if fsErr, err := co.RecvObj(); err != nil {
		panic(err)
	} else if fse, ok := fsErr.(vfs.FsError); !ok {
		panic(errors.Errorf("Unexpected fs error from jdfs with type [%T] - %+v", fsErr, fsErr))
	} else if fse != 0 {
		return syscall.Errno(fse)
	}

	return
}

func (fs *fileSystem) FlushFile(
	ctx context.Context,
	op *vfs.FlushFileOp) (err error) {

	// jdfs won't buffer writes, no need to contact jdfs

	return
}

func (fs *fileSystem) ReleaseFileHandle(
	ctx context.Context,
	op *vfs.ReleaseFileHandleOp) (err error) {
	co, err := fs.po.NewCo()
	if err != nil {
		panic(err)
	}
	defer co.Close()

	if err = co.SendCode(fmt.Sprintf(`
ReleaseFileHandle(%#v)
`, op.Handle)); err != nil {
		panic(err)
	}

	return
}

func (fs *fileSystem) ReadSymlink(
	ctx context.Context,
	op *vfs.ReadSymlinkOp) (err error) {
	co, err := fs.po.NewCo()
	if err != nil {
		panic(err)
	}
	defer co.Close()

	if err = co.SendCode(fmt.Sprintf(`
ReadSymlink(%#v)
`, op.Inode)); err != nil {
		panic(err)
	}

	if err = co.StartRecv(); err != nil {
		panic(err)
	}

	if fsErr, err := co.RecvObj(); err != nil {
		panic(err)
	} else if fse, ok := fsErr.(vfs.FsError); !ok {
		panic(errors.Errorf("Unexpected fs error from jdfs with type [%T] - %+v", fsErr, fsErr))
	} else if fse != 0 {
		return syscall.Errno(fse)
	}

	target, err := co.RecvObj()
	if err != nil {
		panic(err)
	}
	if target, ok := target.(string); !ok {
		panic(errors.Errorf("unexpected target type [%T] of target value [%v]", target, target))
	} else {
		op.Target = target
	}

	return
}

func (fs *fileSystem) RemoveXattr(
	ctx context.Context,
	op *vfs.RemoveXattrOp) (err error) {
	co, err := fs.po.NewCo()
	if err != nil {
		panic(err)
	}
	defer co.Close()

	if err = co.SendCode(fmt.Sprintf(`
RemoveXattr(%#v, %#v)
`, op.Inode, op.Name)); err != nil {
		panic(err)
	}

	if err = co.StartRecv(); err != nil {
		panic(err)
	}

	if fsErr, err := co.RecvObj(); err != nil {
		panic(err)
	} else if fse, ok := fsErr.(vfs.FsError); !ok {
		panic(errors.Errorf("Unexpected fs error from jdfs with type [%T] - %+v", fsErr, fsErr))
	} else if fse != 0 {
		return syscall.Errno(fse)
	}

	return
}

func (fs *fileSystem) GetXattr(
	ctx context.Context,
	op *vfs.GetXattrOp) (err error) {
	co, err := fs.po.NewCo()
	if err != nil {
		panic(err)
	}
	defer co.Close()

	if err = co.SendCode(fmt.Sprintf(`
GetXattr(%#v, %#v, %#v)
`, op.Inode, op.Name, len(op.Dst))); err != nil {
		panic(err)
	}

	if err = co.StartRecv(); err != nil {
		panic(err)
	}

	if fsErr, err := co.RecvObj(); err != nil {
		panic(err)
	} else if fse, ok := fsErr.(vfs.FsError); !ok {
		panic(errors.Errorf("Unexpected fs error from jdfs with type [%T] - %+v", fsErr, fsErr))
	} else if fse != 0 && fse != vfs.ERANGE {
		return syscall.Errno(fse)
	}

	bytesRead, err := co.RecvObj()
	if err != nil {
		panic(err)
	}
	if bytesRead, ok := bytesRead.(hbi.LitIntType); !ok {
		panic(errors.Errorf("unexpected bytesRead type [%T] of bytesRead value [%v]", bytesRead, bytesRead))
	} else {
		op.BytesRead = int(bytesRead)
		if op.BytesRead <= len(op.Dst) {
			if err = co.RecvData(op.Dst[:bytesRead]); err != nil {
				panic(err)
			}
		} else {
			return syscall.ERANGE
		}
	}

	return
}

func (fs *fileSystem) ListXattr(
	ctx context.Context,
	op *vfs.ListXattrOp) (err error) {
	co, err := fs.po.NewCo()
	if err != nil {
		panic(err)
	}
	defer co.Close()

	if err = co.SendCode(fmt.Sprintf(`
ListXattr(%#v, %#v)
`, op.Inode, len(op.Dst))); err != nil {
		panic(err)
	}

	if err = co.StartRecv(); err != nil {
		panic(err)
	}

	if fsErr, err := co.RecvObj(); err != nil {
		panic(err)
	} else if fse, ok := fsErr.(vfs.FsError); !ok {
		panic(errors.Errorf("Unexpected fs error from jdfs with type [%T] - %+v", fsErr, fsErr))
	} else if fse != 0 && fse != vfs.ERANGE {
		return syscall.Errno(fse)
	}

	bytesRead, err := co.RecvObj()
	if err != nil {
		panic(err)
	}
	if bytesRead, ok := bytesRead.(hbi.LitIntType); !ok {
		panic(errors.Errorf("unexpected bytesRead type [%T] of bytesRead value [%v]", bytesRead, bytesRead))
	} else {
		op.BytesRead = int(bytesRead)
		if op.BytesRead <= len(op.Dst) {
			if err = co.RecvData(op.Dst[:bytesRead]); err != nil {
				panic(err)
			}
		} else {
			return syscall.ERANGE
		}
	}

	return
}

func (fs *fileSystem) SetXattr(
	ctx context.Context,
	op *vfs.SetXattrOp) (err error) {
	// allow no space consumption
	err = syscall.ENOSPC
	co, err := fs.po.NewCo()
	if err != nil {
		panic(err)
	}
	defer co.Close()

	if err = co.SendCode(fmt.Sprintf(`
SetXattr(%#v, %#v, %#v, %#v)
`, op.Inode, op.Name, len(op.Value), op.Flags)); err != nil {
		panic(err)
	}
	if err = co.SendData(op.Value); err != nil {
		panic(err)
	}

	if err = co.StartRecv(); err != nil {
		panic(err)
	}

	if fsErr, err := co.RecvObj(); err != nil {
		panic(err)
	} else if fse, ok := fsErr.(vfs.FsError); !ok {
		panic(errors.Errorf("Unexpected fs error from jdfs with type [%T] - %+v", fsErr, fsErr))
	} else if fse != 0 {
		return syscall.Errno(fse)
	}

	return
}

func (fs *fileSystem) Destroy() {
	if fs.po != nil {
		fs.po.Close()
	}
}
