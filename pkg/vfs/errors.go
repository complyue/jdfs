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
	"fmt"
	"os"
	"runtime"
	"syscall"

	"github.com/golang/glog"
)

// FsError is the cross-platform error type for portable filesystem errors.
//
// error values of this type are transfered in literal const name (its Repr()) over HBI wire.
type FsError syscall.Errno

const (
	// EOKAY is the placeholder for no error, this is necessary as a FsError value needs
	// to be exchanged among jdfs/jdfc even on success.
	EOKAY = FsError(0)

	// Errors corresponding to kernel error numbers. These may be treated
	// specially by Connection.Reply.

	EEXIST    = FsError(syscall.EEXIST)
	EINVAL    = FsError(syscall.EINVAL)
	EIO       = FsError(syscall.EIO)
	ENOENT    = FsError(syscall.ENOENT)
	ENOSYS    = FsError(syscall.ENOSYS)
	ENOTDIR   = FsError(syscall.ENOTDIR)
	ENOTEMPTY = FsError(syscall.ENOTEMPTY)
	ERANGE    = FsError(syscall.ERANGE)
	ENOSPC    = FsError(syscall.ENOSPC)

	// ENOATTR and/or ENODATA diverse greatly among OSes,
	// using ENODATA for ENOATTR should work for Linux/macOS/Solaris(SmartOS),
	// some BSDs may not work, but none of BSDs is supported by JDFS so far.
	ENOATTR = FsError(syscall.ENODATA)
)

// implementing builtin error interface
func (fse FsError) Error() string {
	return syscall.Errno(fse).Error()
}

// Repr returns the const name of the error value, for representation to appear in
// peer script as to be executed by HBI interpreters.
func (fse FsError) Repr() string {
	switch fse {
	case EOKAY:
		return "EOKAY"
	case EEXIST:
		return "EEXIST"
	case EINVAL:
		return "EINVAL"
	case EIO:
		return "EIO"
	case ENOENT:
		return "ENOENT"
	case ENOSYS:
		return "ENOSYS"
	case ENOTDIR:
		return "ENOTDIR"
	case ENOTEMPTY:
		return "ENOTEMPTY"
	case ERANGE:
		return "ERANGE"
	case ENOSPC:
		return "ENOSPC"
	case ENOATTR:
		return "ENOATTR"
	}
	panic(fmt.Sprintf("Unexpected file system error number %#x on %s %s - %+v",
		int(fse), runtime.GOOS, runtime.GOARCH, syscall.Errno(fse)))
}

// FsErr converts an arbitrary error occurred on jdfs local filesystem to the portable FsError type
func FsErr(fsErr error) FsError {
	switch fse := fsErr.(type) {
	case nil:
		return EOKAY
	case FsError:
		return fse
	case syscall.Errno:
		return translateSysErrno(fse)
	case *os.PathError:
		return FsErr(fse.Err)
	default:
		glog.Errorf("Unexpected local fs error [%T] - %+v", fsErr, fsErr)
	}
	// use EIO as fallback error
	return EIO
}
