// Package jdfc defines the implementation of Just Data FileSystem client
package jdfc

import (
	"context"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/golang/glog"

	"github.com/complyue/jdfs/pkg/errors"
	"github.com/complyue/jdfs/pkg/fuse"
	"github.com/complyue/jdfs/pkg/vfs"

	"github.com/complyue/hbi"
	"github.com/complyue/hbi/interop"
)

// MountJDFS mounts a remote JDFS filesystem directory (can be root path or sub
// directory under the exported root), to a local mountpoint, then serves
// fs operations over HBI connections between this JDFS client and the
// JDFS server, to be established by jdfsConnector.
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
			glog.Errorf("Unexpected JDFS client error: %+v", err)
		}
		if po != nil && !po.Disconnected() {
			if err != nil {
				po.Disconnect(fmt.Sprintf("Unexpected JDFS client error: %+v", err), true)
			} else {
				po.Close()
			}
		}
	}()

	fs := &fileSystem{
		readOnly: cfg.ReadOnly,
		jdfsPath: jdfsPath,
	}
	mfs, err := fuse.Mount(mountpoint, &fileSystemServer{fs: fs}, cfg)
	if err != nil {
		return err
	}

	if p := mfs.Protocol(); !p.HasInvalidate() {
		err = errors.Errorf("FUSE kernel version %v not supported", p)
		return
	}

	// prepare the hosting environment to be reacting to JDFS server
	he := hbi.NewHostingEnv()
	// expose names for interop
	interop.ExposeInterOpValues(he)
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
		if len(discReason) > 0 {
			fmt.Printf("JDFS server disconnected due to: %s", discReason)
			os.Exit(6)
		}
		// TODO auto reconnect
	})

	if err = dialHBI(); err != nil {
		return err
	}

	if err = mfs.Join(context.Background()); err != nil {
		return err
	}

	return nil
}

type fileSystem struct {
	readOnly bool
	jdfsPath string

	mu sync.Mutex

	po *hbi.PostingEnd
	ho *hbi.HostingEnd

	jdfsRootIno vfs.InodeID
	jdfsUID     int64
	jdfsGID     int64
}

func (fs *fileSystem) NamesToExpose() []string {
	return []string{
		"InvalidateFileContent",
		"InvalidateDirEntry",
	}
}

func (fs *fileSystem) connReset(
	po *hbi.PostingEnd, ho *hbi.HostingEnd,
) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// TODO reset cache, mark all open files stale

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
		fs.jdfsRootIno = vfs.InodeID(mountedFields[0].(hbi.LitIntType))
		fs.jdfsUID = mountedFields[1].(hbi.LitIntType)
		fs.jdfsGID = mountedFields[2].(hbi.LitIntType)

		return
	}(); err != nil {
		fs.po, fs.ho = nil, nil
		glog.Errorf("JDFS server mount failed: %+v", err)
		if !po.Disconnected() {
			po.Disconnect(fmt.Sprintf("server mount failed: %v", err), false)
		}
	}
}

func (fs *fileSystem) InvalidateFileContent(
	inode vfs.InodeID, offset, size int64,
) {
}

func (fs *fileSystem) InvalidateDirEntry(
	dir, inode vfs.InodeID, name string,
) {
}

func (fs *fileSystem) StatFS(
	ctx context.Context,
	op *vfs.StatFSOp) (err error) {
	co, err := fs.po.NewCo()
	if err != nil {
		return err
	}
	defer co.Close()
	if err = co.SendCode(`StatFS()`); err != nil {
		return err
	}
	if err = co.StartRecv(); err != nil {
		return err
	}
	bufView := ((*[unsafe.Sizeof(*op)]byte)(unsafe.Pointer(op)))[0:unsafe.Sizeof(*op)]
	if err = co.RecvData(bufView); err != nil {
		return err
	}
	return
}

func (fs *fileSystem) LookUpInode(
	ctx context.Context,
	op *vfs.LookUpInodeOp) (err error) {
	co, err := fs.po.NewCo()
	if err != nil {
		return err
	}
	defer co.Close()
	if err = co.SendCode(fmt.Sprintf(`
LookUpInode(%#v, %#v)
`, op.Parent, op.Name)); err != nil {
		return err
	}
	if err = co.StartRecv(); err != nil {
		return
	}
	if found, err := co.RecvObj(); err != nil {
		return err
	} else {
		if found.(hbi.LitIntType) == 0 {
			return syscall.ENOENT
		}
	}
	bufView := ((*[unsafe.Sizeof(op.Entry)]byte)(unsafe.Pointer(&op.Entry)))[:unsafe.Sizeof(op.Entry)]
	if err = co.RecvData(bufView); err != nil {
		return err
	}
	return
}

func (fs *fileSystem) GetInodeAttributes(
	ctx context.Context,
	op *vfs.GetInodeAttributesOp) (err error) {
	co, err := fs.po.NewCo()
	if err != nil {
		return err
	}
	defer co.Close()
	if err = co.SendCode(fmt.Sprintf(`
GetInodeAttributes(%#v)
`, op.Inode)); err != nil {
		return err
	}
	if err = co.StartRecv(); err != nil {
		return
	}
	bufView := ((*[unsafe.Sizeof(op.Attributes)]byte)(unsafe.Pointer(&op.Attributes)))[:unsafe.Sizeof(op.Attributes)]
	if err = co.RecvData(bufView); err != nil {
		return err
	}
	op.AttributesExpiration = time.Now().Add(vfs.META_ATTRS_CACHE_TIME)
	return
}

func (fs *fileSystem) SetInodeAttributes(
	ctx context.Context,
	op *vfs.SetInodeAttributesOp) (err error) {
	co, err := fs.po.NewCo()
	if err != nil {
		return err
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
		chgMtimeToNsec = op.Mtime.UnixNano()
	}

	if err = co.SendCode(fmt.Sprintf(`
SetInodeAttributes(%#v,
%#v, %#v, %#v,
%#v, %#v, %#v,
)
`, op.Inode,
		op.Size != nil, op.Mode != nil, op.Mtime != nil,
		chgSizeTo, chgModeTo, chgMtimeToNsec,
	)); err != nil {
		return err
	}

	if err = co.StartRecv(); err != nil {
		return err
	}

	bufView := ((*[unsafe.Sizeof(op.Attributes)]byte)(unsafe.Pointer(&op.Attributes)))[:unsafe.Sizeof(op.Attributes)]
	if err = co.RecvData(bufView); err != nil {
		return err
	}
	op.AttributesExpiration = time.Now().Add(vfs.META_ATTRS_CACHE_TIME)

	return
}

func (fs *fileSystem) ForgetInode(
	ctx context.Context,
	op *vfs.ForgetInodeOp) (err error) {
	co, err := fs.po.NewCo()
	if err != nil {
		return err
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
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) MkNode(
	ctx context.Context,
	op *vfs.MkNodeOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) CreateFile(
	ctx context.Context,
	op *vfs.CreateFileOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) CreateSymlink(
	ctx context.Context,
	op *vfs.CreateSymlinkOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) CreateLink(
	ctx context.Context,
	op *vfs.CreateLinkOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) Rename(
	ctx context.Context,
	op *vfs.RenameOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) RmDir(
	ctx context.Context,
	op *vfs.RmDirOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) Unlink(
	ctx context.Context,
	op *vfs.UnlinkOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) OpenDir(
	ctx context.Context,
	op *vfs.OpenDirOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) ReadDir(
	ctx context.Context,
	op *vfs.ReadDirOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) ReleaseDirHandle(
	ctx context.Context,
	op *vfs.ReleaseDirHandleOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) OpenFile(
	ctx context.Context,
	op *vfs.OpenFileOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) ReadFile(
	ctx context.Context,
	op *vfs.ReadFileOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) WriteFile(
	ctx context.Context,
	op *vfs.WriteFileOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) SyncFile(
	ctx context.Context,
	op *vfs.SyncFileOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) FlushFile(
	ctx context.Context,
	op *vfs.FlushFileOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) ReleaseFileHandle(
	ctx context.Context,
	op *vfs.ReleaseFileHandleOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) ReadSymlink(
	ctx context.Context,
	op *vfs.ReadSymlinkOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) RemoveXattr(
	ctx context.Context,
	op *vfs.RemoveXattrOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) GetXattr(
	ctx context.Context,
	op *vfs.GetXattrOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) ListXattr(
	ctx context.Context,
	op *vfs.ListXattrOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) SetXattr(
	ctx context.Context,
	op *vfs.SetXattrOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) Destroy() {
	fs.po.Close()
}
