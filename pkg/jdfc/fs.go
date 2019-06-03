package jdfc

import (
	"context"
	"io"
	"sync"

	"github.com/complyue/jdfs/pkg/fuse"
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

		s.opsInFlight.Add(1)
		if _, ok := op.(*fuse.ForgetInodeOp); ok {
			// Special case: call in this goroutine for
			// forget inode ops, which may come in a
			// flurry from the kernel and are generally
			// cheap for the file system to handle
			s.handleOp(c, ctx, op)
		} else {
			go s.handleOp(c, ctx, op)
		}
	}
}

func (s *fileSystemServer) handleOp(
	c *fuse.Connection,
	ctx context.Context,
	op interface{}) {
	defer s.opsInFlight.Done()

	// Dispatch to the appropriate method.
	var err error
	switch typed := op.(type) {
	default:
		err = fuse.ENOSYS

	case *fuse.StatFSOp:
		err = s.fs.StatFS(ctx, typed)

	case *fuse.LookUpInodeOp:
		err = s.fs.LookUpInode(ctx, typed)

	case *fuse.GetInodeAttributesOp:
		err = s.fs.GetInodeAttributes(ctx, typed)

	case *fuse.SetInodeAttributesOp:
		err = s.fs.SetInodeAttributes(ctx, typed)

	case *fuse.ForgetInodeOp:
		err = s.fs.ForgetInode(ctx, typed)

	case *fuse.MkDirOp:
		err = s.fs.MkDir(ctx, typed)

	case *fuse.MkNodeOp:
		err = s.fs.MkNode(ctx, typed)

	case *fuse.CreateFileOp:
		err = s.fs.CreateFile(ctx, typed)

	case *fuse.CreateLinkOp:
		err = s.fs.CreateLink(ctx, typed)

	case *fuse.CreateSymlinkOp:
		err = s.fs.CreateSymlink(ctx, typed)

	case *fuse.RenameOp:
		err = s.fs.Rename(ctx, typed)

	case *fuse.RmDirOp:
		err = s.fs.RmDir(ctx, typed)

	case *fuse.UnlinkOp:
		err = s.fs.Unlink(ctx, typed)

	case *fuse.OpenDirOp:
		err = s.fs.OpenDir(ctx, typed)

	case *fuse.ReadDirOp:
		err = s.fs.ReadDir(ctx, typed)

	case *fuse.ReleaseDirHandleOp:
		err = s.fs.ReleaseDirHandle(ctx, typed)

	case *fuse.OpenFileOp:
		err = s.fs.OpenFile(ctx, typed)

	case *fuse.ReadFileOp:
		err = s.fs.ReadFile(ctx, typed)

	case *fuse.WriteFileOp:
		err = s.fs.WriteFile(ctx, typed)

	case *fuse.SyncFileOp:
		err = s.fs.SyncFile(ctx, typed)

	case *fuse.FlushFileOp:
		err = s.fs.FlushFile(ctx, typed)

	case *fuse.ReleaseFileHandleOp:
		err = s.fs.ReleaseFileHandle(ctx, typed)

	case *fuse.ReadSymlinkOp:
		err = s.fs.ReadSymlink(ctx, typed)

	case *fuse.RemoveXattrOp:
		err = s.fs.RemoveXattr(ctx, typed)

	case *fuse.GetXattrOp:
		err = s.fs.GetXattr(ctx, typed)

	case *fuse.ListXattrOp:
		err = s.fs.ListXattr(ctx, typed)

	case *fuse.SetXattrOp:
		err = s.fs.SetXattr(ctx, typed)
	}

	c.Reply(ctx, err)
}
