// Copyright 2019 Compl Yue
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
	"log"
	"os"
	"runtime"
	"syscall"

	"github.com/golang/glog"
)

const (
	// Errors corresponding to kernel error numbers. These may be treated
	// specially by Connection.Reply.

	EEXIST    = syscall.Errno(0x11)
	EINVAL    = syscall.Errno(0x16)
	EIO       = syscall.Errno(0x5)
	ENOENT    = syscall.Errno(0x2)
	ENOSYS    = syscall.Errno(0x4e)
	ENOTDIR   = syscall.Errno(0x14)
	ENOTEMPTY = syscall.Errno(0x42)

	// ENOATTR and ENODATA diverse greatly among OSes,
	// so long as JDFS doesn't support xattr, it's only returned from jdfc to FUSE kernel.
	// jdfs is not supposed to be involved with xattr, so no transfer between jdfs and jdfc.
	ENOATTR = syscall.ENODATA
)

// validate system error number consistency as far as FUSE kernel concerned
func init() {
	if EEXIST != syscall.EEXIST {
		log.Fatalf("FUSE errno EEXIST=%d incompatible with %s %s !", EEXIST, runtime.GOOS, runtime.GOARCH)
	}
	if EINVAL != syscall.EINVAL {
		log.Fatalf("FUSE errno EINVAL=%d incompatible with %s %s !", EINVAL, runtime.GOOS, runtime.GOARCH)
	}
	if EIO != syscall.EIO {
		log.Fatalf("FUSE errno EIO=%d incompatible with %s %s !", EIO, runtime.GOOS, runtime.GOARCH)
	}
	if ENOENT != syscall.ENOENT {
		log.Fatalf("FUSE errno ENOENT=%d incompatible with %s %s !", ENOENT, runtime.GOOS, runtime.GOARCH)
	}
	if ENOSYS != syscall.ENOSYS {
		log.Fatalf("FUSE errno ENOSYS=%d incompatible with %s %s !", ENOSYS, runtime.GOOS, runtime.GOARCH)
	}
	if ENOTDIR != syscall.ENOTDIR {
		log.Fatalf("FUSE errno ENOTDIR=%d incompatible with %s %s !", ENOTDIR, runtime.GOOS, runtime.GOARCH)
	}
	if ENOTEMPTY != syscall.ENOTEMPTY {
		log.Fatalf("FUSE errno ENOTEMPTY=%d incompatible with %s %s !", ENOTEMPTY, runtime.GOOS, runtime.GOARCH)
	}
}

// FsError converts error occurred on jdfs local filesystem to an error number passable to jdfc
func FsError(fsErr error) syscall.Errno {
	switch fse := fsErr.(type) {
	case nil:
		return 0
	case syscall.Errno:
		return fse
	case *os.PathError:
		glog.Errorf("FS operation [%s] on path [%s] failed: [%T] - %+v", fse.Op, fse.Path, fse.Err, fse.Err)
		return FsError(fse.Err)
	default:
		glog.Errorf("Unexpected local fs error [%T] - %+v", fsErr, fsErr)
	}
	return EIO
}
