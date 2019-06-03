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

package fuse

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"reflect"
	"syscall"
	"time"
	"unsafe"
)

////////////////////////////////////////////////////////////////////////
// Incoming messages
////////////////////////////////////////////////////////////////////////

// Convert a kernel message to an appropriate op. If the op is unknown, a
// special unexported type will be used.
//
// The caller is responsible for arranging for the message to be destroyed.
func convertInMessage(
	inMsg *InMessage,
	outMsg *OutMessage,
	protocol Protocol) (o interface{}, err error) {
	switch inMsg.Header().Opcode {
	case OpLookup:
		buf := inMsg.ConsumeBytes(inMsg.Len())
		n := len(buf)
		if n == 0 || buf[n-1] != '\x00' {
			err = errors.New("Corrupt OpLookup")
			return
		}

		o = &LookUpInodeOp{
			Parent: InodeID(inMsg.Header().Nodeid),
			Name:   string(buf[:n-1]),
		}

	case OpGetattr:
		o = &GetInodeAttributesOp{
			Inode: InodeID(inMsg.Header().Nodeid),
		}

	case OpSetattr:
		type input SetattrIn
		in := (*input)(inMsg.Consume(unsafe.Sizeof(input{})))
		if in == nil {
			err = errors.New("Corrupt OpSetattr")
			return
		}

		to := &SetInodeAttributesOp{
			Inode: InodeID(inMsg.Header().Nodeid),
		}
		o = to

		valid := SetattrValid(in.Valid)
		if valid&SetattrSize != 0 {
			to.Size = &in.Size
		}

		if valid&SetattrMode != 0 {
			mode := convertFileMode(in.Mode)
			to.Mode = &mode
		}

		if valid&SetattrAtime != 0 {
			t := time.Unix(int64(in.Atime), int64(in.AtimeNsec))
			to.Atime = &t
		}

		if valid&SetattrMtime != 0 {
			t := time.Unix(int64(in.Mtime), int64(in.MtimeNsec))
			to.Mtime = &t
		}

	case OpForget:
		type input ForgetIn
		in := (*input)(inMsg.Consume(unsafe.Sizeof(input{})))
		if in == nil {
			err = errors.New("Corrupt OpForget")
			return
		}

		o = &ForgetInodeOp{
			Inode: InodeID(inMsg.Header().Nodeid),
			N:     in.Nlookup,
		}

	case OpMkdir:
		in := (*MkdirIn)(inMsg.Consume(MkdirInSize(protocol)))
		if in == nil {
			err = errors.New("Corrupt OpMkdir")
			return
		}

		name := inMsg.ConsumeBytes(inMsg.Len())
		i := bytes.IndexByte(name, '\x00')
		if i < 0 {
			err = errors.New("Corrupt OpMkdir")
			return
		}
		name = name[:i]

		o = &MkDirOp{
			Parent: InodeID(inMsg.Header().Nodeid),
			Name:   string(name),

			// On Linux, vfs_mkdir calls through to the inode with at most
			// permissions and sticky bits set (cf. https://goo.gl/WxgQXk), and fuse
			// passes that on directly (cf. https://goo.gl/f31aMo). In other words,
			// the fact that this is a directory is implicit in the fact that the
			// opcode is mkdir. But we want the correct mode to go through, so ensure
			// that os.ModeDir is set.
			Mode: convertFileMode(in.Mode) | os.ModeDir,
		}

	case OpMknod:
		in := (*MknodIn)(inMsg.Consume(MknodInSize(protocol)))
		if in == nil {
			err = errors.New("Corrupt OpMknod")
			return
		}

		name := inMsg.ConsumeBytes(inMsg.Len())
		i := bytes.IndexByte(name, '\x00')
		if i < 0 {
			err = errors.New("Corrupt OpMknod")
			return
		}
		name = name[:i]

		o = &MkNodeOp{
			Parent: InodeID(inMsg.Header().Nodeid),
			Name:   string(name),
			Mode:   convertFileMode(in.Mode),
		}

	case OpCreate:
		in := (*CreateIn)(inMsg.Consume(CreateInSize(protocol)))
		if in == nil {
			err = errors.New("Corrupt OpCreate")
			return
		}

		name := inMsg.ConsumeBytes(inMsg.Len())
		i := bytes.IndexByte(name, '\x00')
		if i < 0 {
			err = errors.New("Corrupt OpCreate")
			return
		}
		name = name[:i]

		o = &CreateFileOp{
			Parent: InodeID(inMsg.Header().Nodeid),
			Name:   string(name),
			Mode:   convertFileMode(in.Mode),
		}

	case OpSymlink:
		// The message is "newName\0target\0".
		names := inMsg.ConsumeBytes(inMsg.Len())
		if len(names) == 0 || names[len(names)-1] != 0 {
			err = errors.New("Corrupt OpSymlink")
			return
		}
		i := bytes.IndexByte(names, '\x00')
		if i < 0 {
			err = errors.New("Corrupt OpSymlink")
			return
		}
		newName, target := names[0:i], names[i+1:len(names)-1]

		o = &CreateSymlinkOp{
			Parent: InodeID(inMsg.Header().Nodeid),
			Name:   string(newName),
			Target: string(target),
		}

	case OpRename:
		type input RenameIn
		in := (*input)(inMsg.Consume(unsafe.Sizeof(input{})))
		if in == nil {
			err = errors.New("Corrupt OpRename")
			return
		}

		names := inMsg.ConsumeBytes(inMsg.Len())
		// names should be "old\x00new\x00"
		if len(names) < 4 {
			err = errors.New("Corrupt OpRename")
			return
		}
		if names[len(names)-1] != '\x00' {
			err = errors.New("Corrupt OpRename")
			return
		}
		i := bytes.IndexByte(names, '\x00')
		if i < 0 {
			err = errors.New("Corrupt OpRename")
			return
		}
		oldName, newName := names[:i], names[i+1:len(names)-1]

		o = &RenameOp{
			OldParent: InodeID(inMsg.Header().Nodeid),
			OldName:   string(oldName),
			NewParent: InodeID(in.Newdir),
			NewName:   string(newName),
		}

	case OpUnlink:
		buf := inMsg.ConsumeBytes(inMsg.Len())
		n := len(buf)
		if n == 0 || buf[n-1] != '\x00' {
			err = errors.New("Corrupt OpUnlink")
			return
		}

		o = &UnlinkOp{
			Parent: InodeID(inMsg.Header().Nodeid),
			Name:   string(buf[:n-1]),
		}

	case OpRmdir:
		buf := inMsg.ConsumeBytes(inMsg.Len())
		n := len(buf)
		if n == 0 || buf[n-1] != '\x00' {
			err = errors.New("Corrupt OpRmdir")
			return
		}

		o = &RmDirOp{
			Parent: InodeID(inMsg.Header().Nodeid),
			Name:   string(buf[:n-1]),
		}

	case OpOpen:
		o = &OpenFileOp{
			Inode: InodeID(inMsg.Header().Nodeid),
		}

	case OpOpendir:
		o = &OpenDirOp{
			Inode: InodeID(inMsg.Header().Nodeid),
		}

	case OpRead:
		in := (*ReadIn)(inMsg.Consume(ReadInSize(protocol)))
		if in == nil {
			err = errors.New("Corrupt OpRead")
			return
		}

		to := &ReadFileOp{
			Inode:  InodeID(inMsg.Header().Nodeid),
			Handle: HandleID(in.Fh),
			Offset: int64(in.Offset),
		}
		o = to

		readSize := int(in.Size)
		p := outMsg.GrowNoZero(readSize)
		if p == nil {
			err = fmt.Errorf("Can't grow for %d-byte read", readSize)
			return
		}

		sh := (*reflect.SliceHeader)(unsafe.Pointer(&to.Dst))
		sh.Data = uintptr(p)
		sh.Len = readSize
		sh.Cap = readSize

	case OpReaddir:
		in := (*ReadIn)(inMsg.Consume(ReadInSize(protocol)))
		if in == nil {
			err = errors.New("Corrupt OpReaddir")
			return
		}

		to := &ReadDirOp{
			Inode:  InodeID(inMsg.Header().Nodeid),
			Handle: HandleID(in.Fh),
			Offset: DirOffset(in.Offset),
		}
		o = to

		readSize := int(in.Size)
		p := outMsg.GrowNoZero(readSize)
		if p == nil {
			err = fmt.Errorf("Can't grow for %d-byte read", readSize)
			return
		}

		sh := (*reflect.SliceHeader)(unsafe.Pointer(&to.Dst))
		sh.Data = uintptr(p)
		sh.Len = readSize
		sh.Cap = readSize

	case OpRelease:
		type input ReleaseIn
		in := (*input)(inMsg.Consume(unsafe.Sizeof(input{})))
		if in == nil {
			err = errors.New("Corrupt OpRelease")
			return
		}

		o = &ReleaseFileHandleOp{
			Handle: HandleID(in.Fh),
		}

	case OpReleasedir:
		type input ReleaseIn
		in := (*input)(inMsg.Consume(unsafe.Sizeof(input{})))
		if in == nil {
			err = errors.New("Corrupt OpReleasedir")
			return
		}

		o = &ReleaseDirHandleOp{
			Handle: HandleID(in.Fh),
		}

	case OpWrite:
		in := (*WriteIn)(inMsg.Consume(WriteInSize(protocol)))
		if in == nil {
			err = errors.New("Corrupt OpWrite")
			return
		}

		buf := inMsg.ConsumeBytes(inMsg.Len())
		if len(buf) < int(in.Size) {
			err = errors.New("Corrupt OpWrite")
			return
		}

		o = &WriteFileOp{
			Inode:  InodeID(inMsg.Header().Nodeid),
			Handle: HandleID(in.Fh),
			Data:   buf,
			Offset: int64(in.Offset),
		}

	case OpFsync:
		type input FsyncIn
		in := (*input)(inMsg.Consume(unsafe.Sizeof(input{})))
		if in == nil {
			err = errors.New("Corrupt OpFsync")
			return
		}

		o = &SyncFileOp{
			Inode:  InodeID(inMsg.Header().Nodeid),
			Handle: HandleID(in.Fh),
		}

	case OpFlush:
		type input FlushIn
		in := (*input)(inMsg.Consume(unsafe.Sizeof(input{})))
		if in == nil {
			err = errors.New("Corrupt OpFlush")
			return
		}

		o = &FlushFileOp{
			Inode:  InodeID(inMsg.Header().Nodeid),
			Handle: HandleID(in.Fh),
		}

	case OpReadlink:
		o = &ReadSymlinkOp{
			Inode: InodeID(inMsg.Header().Nodeid),
		}

	case OpStatfs:
		o = &StatFSOp{}

	case OpInterrupt:
		type input InterruptIn
		in := (*input)(inMsg.Consume(unsafe.Sizeof(input{})))
		if in == nil {
			err = errors.New("Corrupt OpInterrupt")
			return
		}

		o = &interruptOp{
			FuseID: in.Unique,
		}

	case OpInit:
		type input InitIn
		in := (*input)(inMsg.Consume(unsafe.Sizeof(input{})))
		if in == nil {
			err = errors.New("Corrupt OpInit")
			return
		}

		o = &initOp{
			Kernel:       Protocol{in.Major, in.Minor},
			MaxReadahead: in.MaxReadahead,
			Flags:        InitFlags(in.Flags),
		}

	case OpLink:
		type input LinkIn
		in := (*input)(inMsg.Consume(unsafe.Sizeof(input{})))
		if in == nil {
			err = errors.New("Corrupt OpLink")
			return
		}

		name := inMsg.ConsumeBytes(inMsg.Len())
		i := bytes.IndexByte(name, '\x00')
		if i < 0 {
			err = errors.New("Corrupt OpLink")
			return
		}
		name = name[:i]
		if len(name) == 0 {
			err = errors.New("Corrupt OpLink (Name not read)")
			return
		}

		o = &CreateLinkOp{
			Parent: InodeID(inMsg.Header().Nodeid),
			Name:   string(name),
			Target: InodeID(in.Oldnodeid),
		}

	case OpRemovexattr:
		buf := inMsg.ConsumeBytes(inMsg.Len())
		n := len(buf)
		if n == 0 || buf[n-1] != '\x00' {
			err = errors.New("Corrupt OpRemovexattr")
			return
		}

		o = &RemoveXattrOp{
			Inode: InodeID(inMsg.Header().Nodeid),
			Name:  string(buf[:n-1]),
		}

	case OpGetxattr:
		type input GetxattrIn
		in := (*input)(inMsg.Consume(unsafe.Sizeof(input{})))
		if in == nil {
			err = errors.New("Corrupt OpGetxattr")
			return
		}

		name := inMsg.ConsumeBytes(inMsg.Len())
		i := bytes.IndexByte(name, '\x00')
		if i < 0 {
			err = errors.New("Corrupt OpGetxattr")
			return
		}
		name = name[:i]

		to := &GetXattrOp{
			Inode: InodeID(inMsg.Header().Nodeid),
			Name:  string(name),
		}
		o = to

		readSize := int(in.Size)
		p := outMsg.GrowNoZero(readSize)
		if p == nil {
			err = fmt.Errorf("Can't grow for %d-byte read", readSize)
			return
		}

		sh := (*reflect.SliceHeader)(unsafe.Pointer(&to.Dst))
		sh.Data = uintptr(p)
		sh.Len = readSize
		sh.Cap = readSize

	case OpListxattr:
		type input ListxattrIn
		in := (*input)(inMsg.Consume(unsafe.Sizeof(input{})))
		if in == nil {
			err = errors.New("Corrupt OpListxattr")
			return
		}

		to := &ListXattrOp{
			Inode: InodeID(inMsg.Header().Nodeid),
		}
		o = to

		readSize := int(in.Size)
		if readSize != 0 {
			p := outMsg.GrowNoZero(readSize)
			if p == nil {
				err = fmt.Errorf("Can't grow for %d-byte read", readSize)
				return
			}
			sh := (*reflect.SliceHeader)(unsafe.Pointer(&to.Dst))
			sh.Data = uintptr(p)
			sh.Len = readSize
			sh.Cap = readSize
		}
	case OpSetxattr:
		type input SetxattrIn
		in := (*input)(inMsg.Consume(unsafe.Sizeof(input{})))
		if in == nil {
			err = errors.New("Corrupt OpSetxattr")
			return
		}

		payload := inMsg.ConsumeBytes(inMsg.Len())
		// payload should be "name\x00value"
		if len(payload) < 3 {
			err = errors.New("Corrupt OpSetxattr")
			return
		}
		i := bytes.IndexByte(payload, '\x00')
		if i < 0 {
			err = errors.New("Corrupt OpSetxattr")
			return
		}

		name, value := payload[:i], payload[i+1:len(payload)]

		o = &SetXattrOp{
			Inode: InodeID(inMsg.Header().Nodeid),
			Name:  string(name),
			Value: value,
			Flags: in.Flags,
		}

	default:
		o = &unknownOp{
			OpCode: inMsg.Header().Opcode,
			Inode:  InodeID(inMsg.Header().Nodeid),
		}
	}

	return
}

////////////////////////////////////////////////////////////////////////
// Outgoing messages
////////////////////////////////////////////////////////////////////////

// Fill in the response that should be sent to the kernel, or set noResponse if
// the op requires no response.
func (c *Connection) kernelResponse(
	m *OutMessage,
	fuseID uint64,
	op interface{},
	opErr error) (noResponse bool) {
	h := m.OutHeader()
	h.Unique = fuseID

	// Special case: handle the ops for which the kernel expects no response.
	// interruptOp .
	switch op.(type) {
	case *ForgetInodeOp:
		noResponse = true
		return

	case *interruptOp:
		noResponse = true
		return
	}

	// If the user returned the error, fill in the error field of the outgoing
	// message header.
	if opErr != nil {
		handled := false

		if opErr == syscall.ERANGE {
			switch o := op.(type) {
			case *GetXattrOp:
				writeXattrSize(m, uint32(o.BytesRead))
				handled = true
			case *ListXattrOp:
				writeXattrSize(m, uint32(o.BytesRead))
				handled = true
			}
		}

		if !handled {
			m.OutHeader().Error = -int32(syscall.EIO)
			if errno, ok := opErr.(syscall.Errno); ok {
				m.OutHeader().Error = -int32(errno)
			}

			// Special case: for some types, convertInMessage grew the message in order
			// to obtain a destination  Make sure that we shrink back to just
			// the header, because on OS X the kernel otherwise returns EINVAL when we
			// attempt to write an error response with a length that extends beyond the
			// header.
			m.ShrinkTo(OutMessageHeaderSize)
		}
	}

	// Otherwise, fill in the rest of the response.
	if opErr == nil {
		c.kernelResponseForOp(m, op)
	}

	h.Len = uint32(m.Len())
	return
}

// Like kernelResponse, but assumes the user replied with a nil error to the
// op.
func (c *Connection) kernelResponseForOp(
	m *OutMessage,
	op interface{}) {
	// Create the appropriate output message
	switch o := op.(type) {
	case *LookUpInodeOp:
		size := int(EntryOutSize(c.protocol))
		out := (*EntryOut)(m.Grow(size))
		convertChildInodeEntry(&o.Entry, out)

	case *GetInodeAttributesOp:
		size := int(AttrOutSize(c.protocol))
		out := (*AttrOut)(m.Grow(size))
		out.AttrValid, out.AttrValidNsec = convertExpirationTime(
			o.AttributesExpiration)
		convertAttributes(o.Inode, &o.Attributes, &out.Attr)

	case *SetInodeAttributesOp:
		size := int(AttrOutSize(c.protocol))
		out := (*AttrOut)(m.Grow(size))
		out.AttrValid, out.AttrValidNsec = convertExpirationTime(
			o.AttributesExpiration)
		convertAttributes(o.Inode, &o.Attributes, &out.Attr)

	case *MkDirOp:
		size := int(EntryOutSize(c.protocol))
		out := (*EntryOut)(m.Grow(size))
		convertChildInodeEntry(&o.Entry, out)

	case *MkNodeOp:
		size := int(EntryOutSize(c.protocol))
		out := (*EntryOut)(m.Grow(size))
		convertChildInodeEntry(&o.Entry, out)

	case *CreateFileOp:
		eSize := int(EntryOutSize(c.protocol))

		e := (*EntryOut)(m.Grow(eSize))
		convertChildInodeEntry(&o.Entry, e)

		oo := (*OpenOut)(m.Grow(int(unsafe.Sizeof(OpenOut{}))))
		oo.Fh = uint64(o.Handle)

	case *CreateSymlinkOp:
		size := int(EntryOutSize(c.protocol))
		out := (*EntryOut)(m.Grow(size))
		convertChildInodeEntry(&o.Entry, out)

	case *CreateLinkOp:
		size := int(EntryOutSize(c.protocol))
		out := (*EntryOut)(m.Grow(size))
		convertChildInodeEntry(&o.Entry, out)

	case *RenameOp:
		// Empty response

	case *RmDirOp:
		// Empty response

	case *UnlinkOp:
		// Empty response

	case *OpenDirOp:
		out := (*OpenOut)(m.Grow(int(unsafe.Sizeof(OpenOut{}))))
		out.Fh = uint64(o.Handle)

	case *ReadDirOp:
		// convertInMessage already set up the destination buffer to be at the end
		// of the out message. We need only shrink to the right size based on how
		// much the user read.
		m.ShrinkTo(OutMessageHeaderSize + o.BytesRead)

	case *ReleaseDirHandleOp:
		// Empty response

	case *OpenFileOp:
		out := (*OpenOut)(m.Grow(int(unsafe.Sizeof(OpenOut{}))))
		out.Fh = uint64(o.Handle)

		if o.KeepPageCache {
			out.OpenFlags |= uint32(OpenKeepCache)
		}

		if o.UseDirectIO {
			out.OpenFlags |= uint32(OpenDirectIO)
		}

	case *ReadFileOp:
		// convertInMessage already set up the destination buffer to be at the end
		// of the out message. We need only shrink to the right size based on how
		// much the user read.
		m.ShrinkTo(OutMessageHeaderSize + o.BytesRead)

	case *WriteFileOp:
		out := (*WriteOut)(m.Grow(int(unsafe.Sizeof(WriteOut{}))))
		out.Size = uint32(len(o.Data))

	case *SyncFileOp:
		// Empty response

	case *FlushFileOp:
		// Empty response

	case *ReleaseFileHandleOp:
		// Empty response

	case *ReadSymlinkOp:
		m.AppendString(o.Target)

	case *StatFSOp:
		out := (*StatfsOut)(m.Grow(int(unsafe.Sizeof(StatfsOut{}))))
		out.St.Blocks = o.Blocks
		out.St.Bfree = o.BlocksFree
		out.St.Bavail = o.BlocksAvailable
		out.St.Files = o.Inodes
		out.St.Ffree = o.InodesFree

		// The posix spec for sys/statvfs.h (http://goo.gl/LktgrF) defines the
		// following fields of statvfs, among others:
		//
		//     f_bsize    File system block size.
		//     f_frsize   Fundamental file system block size.
		//     f_blocks   Total number of blocks on file system in units of f_frsize.
		//
		// It appears as though f_bsize was the only thing supported by most unixes
		// originally, but then f_frsize was added when new sorts of file systems
		// came about. Quoth The Linux Programming Interface by Michael Kerrisk
		// (https://goo.gl/5LZMxQ):
		//
		//     For most Linux file systems, the values of f_bsize and f_frsize are
		//     the same. However, some file systems support the notion of block
		//     fragments, which can be used to allocate a smaller unit of storage
		//     at the end of the file if if a full block is not required. This
		//     avoids the waste of space that would otherwise occur if a full block
		//     was allocated. On such file systems, f_frsize is the size of a
		//     fragment, and f_bsize is the size of a whole block. (The notion of
		//     fragments in UNIX file systems first appeared in the early 1980s
		//     with the 4.2BSD Fast File System.)
		//
		// Confusingly, it appears as though osxfuse surfaces fuse_kstatfs::bsize
		// as statfs::f_iosize (of advisory use only), and fuse_kstatfs::frsize as
		// statfs::f_bsize (which affects free space display in the Finder).
		out.St.Bsize = o.IoSize
		out.St.Frsize = o.BlockSize

	case *RemoveXattrOp:
		// Empty response

	case *GetXattrOp:
		// convertInMessage already set up the destination buffer to be at the end
		// of the out message. We need only shrink to the right size based on how
		// much the user read.
		if o.BytesRead == 0 {
			writeXattrSize(m, uint32(o.BytesRead))
		} else {
			m.ShrinkTo(OutMessageHeaderSize + o.BytesRead)
		}

	case *ListXattrOp:
		if o.BytesRead == 0 {
			writeXattrSize(m, uint32(o.BytesRead))
		} else {
			m.ShrinkTo(OutMessageHeaderSize + o.BytesRead)
		}

	case *SetXattrOp:
		// Empty response

	case *initOp:
		out := (*InitOut)(m.Grow(int(unsafe.Sizeof(InitOut{}))))

		out.Major = o.Library.Major
		out.Minor = o.Library.Minor
		out.MaxReadahead = o.MaxReadahead
		out.Flags = uint32(o.Flags)
		out.MaxWrite = o.MaxWrite

	default:
		panic(fmt.Sprintf("Unexpected op: %#v", op))
	}

	return
}

////////////////////////////////////////////////////////////////////////
// General conversions
////////////////////////////////////////////////////////////////////////

func convertTime(t time.Time) (secs uint64, nsec uint32) {
	totalNano := t.UnixNano()
	secs = uint64(totalNano / 1e9)
	nsec = uint32(totalNano % 1e9)
	return
}

func convertAttributes(
	inodeID InodeID,
	in *InodeAttributes,
	out *Attr) {
	out.Ino = uint64(inodeID)
	out.Size = in.Size
	out.Atime, out.AtimeNsec = convertTime(in.Atime)
	out.Mtime, out.MtimeNsec = convertTime(in.Mtime)
	out.Ctime, out.CtimeNsec = convertTime(in.Ctime)
	out.SetCrtime(convertTime(in.Crtime))
	out.Nlink = in.Nlink
	out.Uid = in.Uid
	out.Gid = in.Gid
	// round up to the nearest 512 boundary
	out.Blocks = (in.Size + 512 - 1) / 512

	// Set the mode.
	out.Mode = uint32(in.Mode) & 0777
	switch {
	default:
		out.Mode |= syscall.S_IFREG
	case in.Mode&os.ModeDir != 0:
		out.Mode |= syscall.S_IFDIR
	case in.Mode&os.ModeDevice != 0:
		if in.Mode&os.ModeCharDevice != 0 {
			out.Mode |= syscall.S_IFCHR
		} else {
			out.Mode |= syscall.S_IFBLK
		}
	case in.Mode&os.ModeNamedPipe != 0:
		out.Mode |= syscall.S_IFIFO
	case in.Mode&os.ModeSymlink != 0:
		out.Mode |= syscall.S_IFLNK
	case in.Mode&os.ModeSocket != 0:
		out.Mode |= syscall.S_IFSOCK
	}
}

// Convert an absolute cache expiration time to a relative time from now for
// consumption by the fuse kernel module.
func convertExpirationTime(t time.Time) (secs uint64, nsecs uint32) {
	// Fuse represents durations as unsigned 64-bit counts of seconds and 32-bit
	// counts of nanoseconds (cf. http://goo.gl/EJupJV). So negative durations
	// are right out. There is no need to cap the positive magnitude, because
	// 2^64 seconds is well longer than the 2^63 ns range of time.Duration.
	d := t.Sub(time.Now())
	if d > 0 {
		secs = uint64(d / time.Second)
		nsecs = uint32((d % time.Second) / time.Nanosecond)
	}

	return
}

func convertChildInodeEntry(
	in *ChildInodeEntry,
	out *EntryOut) {
	out.Nodeid = uint64(in.Child)
	out.Generation = uint64(in.Generation)
	out.EntryValid, out.EntryValidNsec = convertExpirationTime(in.EntryExpiration)
	out.AttrValid, out.AttrValidNsec = convertExpirationTime(in.AttributesExpiration)

	convertAttributes(in.Child, &in.Attributes, &out.Attr)
}

func convertFileMode(unixMode uint32) os.FileMode {
	mode := os.FileMode(unixMode & 0777)
	switch unixMode & syscall.S_IFMT {
	case syscall.S_IFREG:
		// nothing
	case syscall.S_IFDIR:
		mode |= os.ModeDir
	case syscall.S_IFCHR:
		mode |= os.ModeCharDevice | os.ModeDevice
	case syscall.S_IFBLK:
		mode |= os.ModeDevice
	case syscall.S_IFIFO:
		mode |= os.ModeNamedPipe
	case syscall.S_IFLNK:
		mode |= os.ModeSymlink
	case syscall.S_IFSOCK:
		mode |= os.ModeSocket
	default:
		// no idea
		mode |= os.ModeDevice
	}
	if unixMode&syscall.S_ISUID != 0 {
		mode |= os.ModeSetuid
	}
	if unixMode&syscall.S_ISGID != 0 {
		mode |= os.ModeSetgid
	}
	return mode
}

func writeXattrSize(m *OutMessage, size uint32) {
	out := (*GetxattrOut)(m.Grow(int(unsafe.Sizeof(GetxattrOut{}))))
	out.Size = size
}
