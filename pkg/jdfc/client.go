// Package jdfc defines the implementation of Just Data FileSystem client
package jdfc

import (
	"context"
	"fmt"
	"sync"

	"github.com/golang/glog"

	"github.com/complyue/jdfs/pkg/errors"
	"github.com/complyue/jdfs/pkg/fuse"

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
	jdfsPath string

	po *hbi.PostingEnd
	ho *hbi.HostingEnd

	mu sync.Mutex
}

func (fs *fileSystem) NamesToExpose() []string {
	return []string{
		"Mounted",
		"InvalidateFileContent",
		"InvalidateDirEntry",
	}
}

func (fs *fileSystem) connReset(
	po *hbi.PostingEnd, ho *hbi.HostingEnd,
) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// TODO mark all open files stale

	fs.po, fs.ho = po, ho

	// TODO, reset cache, initialize root node
}

func (fs *fileSystem) Mounted(rootInode uint64, servUID, servGID int) {
	//

}

func (fs *fileSystem) InvalidateFileContent(
	inode fuse.InodeID, offset, size int64,
) {
}

func (fs *fileSystem) InvalidateDirEntry(
	dir, inode fuse.InodeID, name string,
) {
}

func (fs *fileSystem) StatFS(
	ctx context.Context,
	op *fuse.StatFSOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) LookUpInode(
	ctx context.Context,
	op *fuse.LookUpInodeOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) GetInodeAttributes(
	ctx context.Context,
	op *fuse.GetInodeAttributesOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) SetInodeAttributes(
	ctx context.Context,
	op *fuse.SetInodeAttributesOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) ForgetInode(
	ctx context.Context,
	op *fuse.ForgetInodeOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) MkDir(
	ctx context.Context,
	op *fuse.MkDirOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) MkNode(
	ctx context.Context,
	op *fuse.MkNodeOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) CreateFile(
	ctx context.Context,
	op *fuse.CreateFileOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) CreateSymlink(
	ctx context.Context,
	op *fuse.CreateSymlinkOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) CreateLink(
	ctx context.Context,
	op *fuse.CreateLinkOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) Rename(
	ctx context.Context,
	op *fuse.RenameOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) RmDir(
	ctx context.Context,
	op *fuse.RmDirOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) Unlink(
	ctx context.Context,
	op *fuse.UnlinkOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) OpenDir(
	ctx context.Context,
	op *fuse.OpenDirOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) ReadDir(
	ctx context.Context,
	op *fuse.ReadDirOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) ReleaseDirHandle(
	ctx context.Context,
	op *fuse.ReleaseDirHandleOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) OpenFile(
	ctx context.Context,
	op *fuse.OpenFileOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) ReadFile(
	ctx context.Context,
	op *fuse.ReadFileOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) WriteFile(
	ctx context.Context,
	op *fuse.WriteFileOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) SyncFile(
	ctx context.Context,
	op *fuse.SyncFileOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) FlushFile(
	ctx context.Context,
	op *fuse.FlushFileOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) ReleaseFileHandle(
	ctx context.Context,
	op *fuse.ReleaseFileHandleOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) ReadSymlink(
	ctx context.Context,
	op *fuse.ReadSymlinkOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) RemoveXattr(
	ctx context.Context,
	op *fuse.RemoveXattrOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) GetXattr(
	ctx context.Context,
	op *fuse.GetXattrOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) ListXattr(
	ctx context.Context,
	op *fuse.ListXattrOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) SetXattr(
	ctx context.Context,
	op *fuse.SetXattrOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *fileSystem) Destroy() {
}
