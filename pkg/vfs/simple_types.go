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

package vfs

import (
	"fmt"
	"os"
)

// InodeID is a 64-bit number used to uniquely identify a file or directory in
// the file system. File systems may mint inode IDs with any value except for
// RootInodeID.
//
// This corresponds to struct inode::i_no in the VFS layer.
// (Cf. http://goo.gl/tvYyQt)
type InodeID uint64

// RootInodeID is a distinguished inode ID that identifies the root of the file
// system, e.g. in an OpenDirOp or LookUpInodeOp. Unlike all other inode IDs,
// which are minted by the file system, the FUSE VFS layer may send a request
// for this ID without the file system ever having referenced it in a previous
// response.
const RootInodeID = 1

// InodeAttributes contains attributes for a file or directory inode. It
// corresponds to struct inode (cf. http://goo.gl/tvYyQt).
type InodeAttributes struct {
	Size uint64

	// The number of incoming hard links to this inode.
	Nlink uint32

	// The mode of the inode. This is exposed to the user in e.g. the result of
	// fstat(2).
	//
	// Note that in contrast to the defaults for FUSE, this package mounts file
	// systems in a manner such that the kernel checks inode permissions in the
	// standard posix way. This is implemented by setting the default_permissions
	// mount option (cf. http://goo.gl/1LxOop and http://goo.gl/1pTjuk).
	//
	// For example, in the case of mkdir:
	//
	//  *  (http://goo.gl/JkdxDI) sys_mkdirat calls inode_permission.
	//
	//  *  (...) inode_permission eventually calls do_inode_permission.
	//
	//  *  (http://goo.gl/aGCsmZ) calls i_op->permission, which is
	//     fuse_permission (cf. http://goo.gl/VZ9beH).
	//
	//  *  (http://goo.gl/5kqUKO) fuse_permission doesn't do anything at all for
	//     several code paths if FUSE_DEFAULT_PERMISSIONS is unset. In contrast,
	//     if that flag *is* set, then it calls generic_permission.
	//
	Mode os.FileMode

	// Time information. See `man 2 stat` for full details.
	Atime  int64 // Time of last access
	Mtime  int64 // Time of last modification
	Ctime  int64 // Time of last modification to inode
	Crtime int64 // Time of creation (OS X only)

	// Ownership information
	Uid uint32
	Gid uint32
}

func (a *InodeAttributes) DebugString() string {
	return fmt.Sprintf(
		"%d %d %v %d %d",
		a.Size,
		a.Nlink,
		a.Mode,
		a.Uid,
		a.Gid)
}

// GenerationNumber represents a generation of an inode. It is irrelevant for
// file systems that won't be exported over NFS. For those that will and that
// reuse inode IDs when they become free, the generation number must change
// when an ID is reused.
//
// This corresponds to struct inode::i_generation in the VFS layer.
// (Cf. http://goo.gl/tvYyQt)
//
// Some related reading:
//
//     http://fuse.sourceforge.net/doxygen/structfuse__entry__param.html
//     http://stackoverflow.com/q/11071996/1505451
//     http://goo.gl/CqvwyX
//     http://julipedia.meroh.net/2005/09/nfs-file-handles.html
//     http://goo.gl/wvo3MB
//
type GenerationNumber uint64

// HandleID is an opaque 64-bit number used to identify a particular open
// handle to a file or directory.
//
// This corresponds to fuse_file_info::fh.
type HandleID uint64

// DirOffset is an offset into an open directory handle. This is opaque to
// FUSE, and can be used for whatever purpose the file system desires. See
// notes on ReadDirOp.Offset for details.
type DirOffset uint64

// ChildInodeEntry contains information about a child inode within its parent
// directory. It is shared by LookUpInodeOp, MkDirOp, CreateFileOp, etc, and is
// consumed by the kernel in order to set up a dcache entry.
type ChildInodeEntry struct {
	// The ID of the child inode. The file system must ensure that the returned
	// inode ID remains valid until a later ForgetInodeOp.
	Child InodeID

	// A generation number for this incarnation of the inode with the given ID.
	// See comments on type GenerationNumber for more.
	Generation GenerationNumber

	// Current attributes for the child inode.
	//
	// When creating a new inode, the file system is responsible for initializing
	// and recording (where supported) attributes like time information,
	// ownership information, etc.
	//
	// Ownership information in particular must be set to something reasonable or
	// by default root will own everything and unprivileged users won't be able
	// to do anything useful. In traditional file systems in the kernel, the
	// function inode_init_owner (http://goo.gl/5qavg8) contains the
	// standards-compliant logic for this.
	Attributes InodeAttributes
}
