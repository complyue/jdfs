// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fuseutil

import (
	"context"
	"io"
	"sync"

	"github.com/complyue/jdfs/pkg/fuse"
)

// An interface with a method for each op type in the fuseops package. This can
// be used in conjunction with NewFileSystemServer to avoid writing a "dispatch
// loop" that switches on op types, instead receiving typed method calls
// directly.
//
// The FileSystem implementation should not call Connection.Reply, instead
// returning the error with which the caller should respond.
//
// See NotImplementedFileSystem for a convenient way to embed default
// implementations for methods you don't care about.
type FileSystem interface {
	StatFS(context.Context, *fuse.StatFSOp) error
	LookUpInode(context.Context, *fuse.LookUpInodeOp) error
	GetInodeAttributes(context.Context, *fuse.GetInodeAttributesOp) error
	SetInodeAttributes(context.Context, *fuse.SetInodeAttributesOp) error
	ForgetInode(context.Context, *fuse.ForgetInodeOp) error
	MkDir(context.Context, *fuse.MkDirOp) error
	MkNode(context.Context, *fuse.MkNodeOp) error
	CreateFile(context.Context, *fuse.CreateFileOp) error
	CreateLink(context.Context, *fuse.CreateLinkOp) error
	CreateSymlink(context.Context, *fuse.CreateSymlinkOp) error
	Rename(context.Context, *fuse.RenameOp) error
	RmDir(context.Context, *fuse.RmDirOp) error
	Unlink(context.Context, *fuse.UnlinkOp) error
	OpenDir(context.Context, *fuse.OpenDirOp) error
	ReadDir(context.Context, *fuse.ReadDirOp) error
	ReleaseDirHandle(context.Context, *fuse.ReleaseDirHandleOp) error
	OpenFile(context.Context, *fuse.OpenFileOp) error
	ReadFile(context.Context, *fuse.ReadFileOp) error
	WriteFile(context.Context, *fuse.WriteFileOp) error
	SyncFile(context.Context, *fuse.SyncFileOp) error
	FlushFile(context.Context, *fuse.FlushFileOp) error
	ReleaseFileHandle(context.Context, *fuse.ReleaseFileHandleOp) error
	ReadSymlink(context.Context, *fuse.ReadSymlinkOp) error
	RemoveXattr(context.Context, *fuse.RemoveXattrOp) error
	GetXattr(context.Context, *fuse.GetXattrOp) error
	ListXattr(context.Context, *fuse.ListXattrOp) error
	SetXattr(context.Context, *fuse.SetXattrOp) error

	// Regard all inodes (including the root inode) as having their lookup counts
	// decremented to zero, and clean up any resources associated with the file
	// system. No further calls to the file system will be made.
	Destroy()
}

// Create a fuse.Server that handles ops by calling the associated FileSystem
// method.Respond with the resulting error. Unsupported ops are responded to
// directly with ENOSYS.
//
// Each call to a FileSystem method (except ForgetInode) is made on
// its own goroutine, and is free to block. ForgetInode may be called
// synchronously, and should not depend on calls to other methods
// being received concurrently.
//
// (It is safe to naively process ops concurrently because the kernel
// guarantees to serialize operations that the user expects to happen in order,
// cf. http://goo.gl/jnkHPO, fuse-devel thread "Fuse guarantees on concurrent
// requests").
func NewFileSystemServer(fs FileSystem) fuse.Server {
	return &fileSystemServer{
		fs: fs,
	}
}

type fileSystemServer struct {
	fs          FileSystem
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
