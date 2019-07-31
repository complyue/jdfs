package vfs

import "syscall"

func translateSysErrno(sysErrno syscall.Errno) FsError {
	switch sysErrno {
	default:
		return FsError(sysErrno)
	}
}
