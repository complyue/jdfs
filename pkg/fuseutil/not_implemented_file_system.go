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

	"github.com/complyue/jdfs/pkg/fuse"
)

// A FileSystem that responds to all ops with fuse.ENOSYS. Embed this in your
// struct to inherit default implementations for the methods you don't care
// about, ensuring your struct will continue to implement FileSystem even as
// new methods are added.
type NotImplementedFileSystem struct {
}

var _ FileSystem = &NotImplementedFileSystem{}

func (fs *NotImplementedFileSystem) StatFS(
	ctx context.Context,
	op *fuse.StatFSOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) LookUpInode(
	ctx context.Context,
	op *fuse.LookUpInodeOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) GetInodeAttributes(
	ctx context.Context,
	op *fuse.GetInodeAttributesOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) SetInodeAttributes(
	ctx context.Context,
	op *fuse.SetInodeAttributesOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) ForgetInode(
	ctx context.Context,
	op *fuse.ForgetInodeOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) MkDir(
	ctx context.Context,
	op *fuse.MkDirOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) MkNode(
	ctx context.Context,
	op *fuse.MkNodeOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) CreateFile(
	ctx context.Context,
	op *fuse.CreateFileOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) CreateSymlink(
	ctx context.Context,
	op *fuse.CreateSymlinkOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) CreateLink(
	ctx context.Context,
	op *fuse.CreateLinkOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) Rename(
	ctx context.Context,
	op *fuse.RenameOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) RmDir(
	ctx context.Context,
	op *fuse.RmDirOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) Unlink(
	ctx context.Context,
	op *fuse.UnlinkOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) OpenDir(
	ctx context.Context,
	op *fuse.OpenDirOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) ReadDir(
	ctx context.Context,
	op *fuse.ReadDirOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) ReleaseDirHandle(
	ctx context.Context,
	op *fuse.ReleaseDirHandleOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) OpenFile(
	ctx context.Context,
	op *fuse.OpenFileOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) ReadFile(
	ctx context.Context,
	op *fuse.ReadFileOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) WriteFile(
	ctx context.Context,
	op *fuse.WriteFileOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) SyncFile(
	ctx context.Context,
	op *fuse.SyncFileOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) FlushFile(
	ctx context.Context,
	op *fuse.FlushFileOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) ReleaseFileHandle(
	ctx context.Context,
	op *fuse.ReleaseFileHandleOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) ReadSymlink(
	ctx context.Context,
	op *fuse.ReadSymlinkOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) RemoveXattr(
	ctx context.Context,
	op *fuse.RemoveXattrOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) GetXattr(
	ctx context.Context,
	op *fuse.GetXattrOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) ListXattr(
	ctx context.Context,
	op *fuse.ListXattrOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) SetXattr(
	ctx context.Context,
	op *fuse.SetXattrOp) (err error) {
	err = fuse.ENOSYS
	return
}

func (fs *NotImplementedFileSystem) Destroy() {
}
