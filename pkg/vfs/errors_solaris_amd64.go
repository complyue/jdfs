package vfs

import "syscall"

func translateSysErrno(sysErrno syscall.Errno) FsError {
	switch sysErrno {
	case syscall.EDQUOT:
		// disc quota exceeded
		return ENOSPC
	default:
		return FsError(sysErrno)
	}
}
