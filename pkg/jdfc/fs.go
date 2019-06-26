package jdfc

import (
	"context"
	"io"
	"sync"
	"syscall"

	"github.com/complyue/jdfs/pkg/errors"
	"github.com/complyue/jdfs/pkg/fuse"
	"github.com/complyue/jdfs/pkg/vfs"
)

type fileSystemServer struct {
	fs          *fileSystem
	opsInFlight sync.WaitGroup
}

func (s *fileSystemServer) ServeOps(c *fuse.Connection) {
	// When we are done, we clean up by waiting for all in-flight ops then
	// destroying the file system.
	defer func() {
		s.opsInFlight.Wait()
		s.fs.Destroy()
	}()

	for {
		ctx, op, err := c.ReadOp()
		if err == io.EOF {
			break
		}

		if err != nil {
			panic(err)
		}

		if _, ok := op.(*fuse.DestroyOp); ok {
			break
		}

		s.opsInFlight.Add(1)
		go s.handleOp(c, ctx, op)
	}
}

func (s *fileSystemServer) handleOp(
	c *fuse.Connection,
	ctx context.Context,
	op interface{}) {
	defer s.opsInFlight.Done()

	var postJob func() error

	// Dispatch to the appropriate method.
	var err error
	switch typed := op.(type) {
	default:
		err = vfs.ENOSYS

	case *vfs.StatFSOp:
		err = s.fs.StatFS(ctx, typed)

	case *vfs.LookUpInodeOp:
		err = s.fs.LookUpInode(ctx, typed)

	case *vfs.GetInodeAttributesOp:
		err = s.fs.GetInodeAttributes(ctx, typed)

	case *vfs.SetInodeAttributesOp:
		err = s.fs.SetInodeAttributes(ctx, typed)

	case *vfs.ForgetInodeOp:
		err = s.fs.ForgetInode(ctx, typed)

	case *vfs.MkDirOp:
		err = s.fs.MkDir(ctx, typed)

	case *vfs.MkNodeOp:
		err = s.fs.MkNode(ctx, typed)

	case *vfs.CreateFileOp:
		err = s.fs.CreateFile(ctx, typed)

	case *vfs.CreateLinkOp:
		err = s.fs.CreateLink(ctx, typed)

	case *vfs.CreateSymlinkOp:
		err = s.fs.CreateSymlink(ctx, typed)

	case *vfs.RenameOp:
		err = s.fs.Rename(ctx, typed)

	case *vfs.RmDirOp:
		err = s.fs.RmDir(ctx, typed)

	case *vfs.UnlinkOp:
		err = s.fs.Unlink(ctx, typed)

	case *vfs.OpenDirOp:
		err = s.fs.OpenDir(ctx, typed)

	case *vfs.ReadDirOp:
		err = s.fs.ReadDir(ctx, typed)

	case *vfs.ReleaseDirHandleOp:
		err = s.fs.ReleaseDirHandle(ctx, typed)

	case *vfs.OpenFileOp:
		err = s.fs.OpenFile(ctx, typed)

	case *vfs.ReadFileOp:
		err = s.fs.ReadFile(ctx, typed)

	case *vfs.WriteFileOp:
		err = s.fs.WriteFile(ctx, typed)

	case *vfs.SyncFileOp:
		err = s.fs.SyncFile(ctx, typed)

	case *vfs.FlushFileOp:
		err = s.fs.FlushFile(ctx, typed)

		postJob = func() error {
			// FUSE kernel is not smart enough to infer file size increasing from write operations.
			// we invalidate attrs cache on flush (i.e. handle close), so the kernel knows it needs
			// to contact jdfs for new file size. if kernel not to update the cached file size,
			// programs like `git clone` won't work.
			//
			// use negative offset to avoid invalidation of page cache, both macOS and Linux drop
			// page cache even with offset==0 && len==0
			// https://github.com/torvalds/linux/blob/4ae004a9bca8bef118c2b4e76ee31c7df4514f18/fs/fuse/inode.c#L344
			if err := c.InvalidateNode(typed.Inode, -1, 0); err != nil && err != syscall.ENOENT {
				return errors.Wrapf(err, "Unexpected fuse kernel error on inode invalidation [%T]", err)
			}
			return nil
		}

	case *vfs.ReleaseFileHandleOp:
		err = s.fs.ReleaseFileHandle(ctx, typed)

	case *vfs.ReadSymlinkOp:
		err = s.fs.ReadSymlink(ctx, typed)

	case *vfs.RemoveXattrOp:
		err = s.fs.RemoveXattr(ctx, typed)

	case *vfs.GetXattrOp:
		err = s.fs.GetXattr(ctx, typed)

	case *vfs.ListXattrOp:
		err = s.fs.ListXattr(ctx, typed)

	case *vfs.SetXattrOp:
		err = s.fs.SetXattr(ctx, typed)
	}

	c.Reply(ctx, err)

	if postJob != nil {
		if err = postJob(); err != nil {
			panic(err)
		}
	}
}
