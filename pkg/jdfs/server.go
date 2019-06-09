// Package jdfs defines implementation of the Just Data FileSystem server
package jdfs

import (
	"fmt"
	"os"
	"unsafe"

	"github.com/complyue/jdfs/pkg/fuse"

	"github.com/complyue/hbi"
)

type exportedFileSystem struct {
	// the root directory that this JDFS server is willing to export.
	//
	// a JDFS client can mount jdfsPath="/" for this root directory,
	// or it can mount any sub dir under this path.
	//
	// multiple local filesystems can be separately mounted under this path for different
	// JDFS clients to mount.
	//
	// TODO for a JDFS mount to expose nested filesystems under its mounted root dir,
	// there're possibilities that inode numbers from different fs collide, maybe FUSE
	// generationNumber can be used to support that, or just don't support nested fs over
	// JDFS.
	exportRoot string

	// HBI posting/hosting ends
	po *hbi.PostingEnd
	ho *hbi.HostingEnd

	// effective uid/gid of JDFS server process, this is told to JDFS client when initially
	// mounted, JDFS client is supposed to translate all inode owner uid/gid of these values
	// to its FUSE uid/gid as exposed to client kernel/applications, so the owning uid/gid of
	// inodes stored in the backing fs at JDFS server can be different from the FUSE uid/gid
	// at JDFS client, while those files/dirs appear owned by the FUSE uid/gid.
	//
	// TODO decide handling of uid/gid other than these values, to leave them as is, or
	//      maybe a good idea to translate to a fixed value (e.g. 0=root, 1=daemon) ?
	jdfsUID, jdfsGID int

	// whether readOnly, as JDFS client requested on initial mount
	readOnly bool

	// in-core filesystem data
	icd icFSD
}

func (efs *exportedFileSystem) NamesToExpose() []string {
	return []string{
		"Mount", "StatFS", "LookUpInode",
	}
}

func (efs *exportedFileSystem) Mount(readOnly bool, jdfsPath string) {
	efs.jdfsUID = os.Geteuid()
	efs.jdfsGID = os.Getegid()

	efs.readOnly = readOnly

	var rootPath string
	if jdfsPath == "/" || jdfsPath == "" {
		rootPath = efs.exportRoot
	} else {
		rootPath = efs.exportRoot + jdfsPath
	}

	if err := os.Chdir(rootPath); err != nil {
		panic(err)
	}

	if err := efs.icd.init(rootPath, readOnly); err != nil {
		efs.ho.Disconnect(fmt.Sprintf("%s", err), true)
		panic(err)
	}

	co := efs.ho.Co()
	if err := co.StartSend(); err != nil {
		panic(err)
	}

	// send mount result fields
	if err := co.SendObj(hbi.Repr(hbi.LitListType{
		efs.icd.rootInode, efs.jdfsUID, efs.jdfsGID,
	})); err != nil {
		panic(err)
	}
}

func (efs *exportedFileSystem) StatFS() {
	co := efs.ho.Co()

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	var op fuse.StatFSOp

	op, err := statFS(efs.icd.rootDir)
	if err != nil {
		panic(err)
	}

	bufView := ((*[unsafe.Sizeof(op)]byte)(unsafe.Pointer(&op)))[0:unsafe.Sizeof(op)]
	if err := co.SendData(bufView); err != nil {
		panic(err)
	}
}

func (efs *exportedFileSystem) LookUpInode(parent InodeID, name string) {
	co := efs.ho.Co()

	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	if parent == fuse.RootID { // translate FUSE root to actual root inode
		parent = efs.icd.rootInode
	}
	ce := efs.icd.LookUpInode(parent, name)

	if err := co.StartSend(); err != nil {
		panic(err)
	}

	if ce == nil {
		if err := co.SendObj(`0`); err != nil {
			panic(err)
		}
		return
	}

	if err := co.SendObj(`1`); err != nil {
		panic(err)
	}
	bufView := ((*[unsafe.Sizeof(*ce)]byte)(unsafe.Pointer(ce)))[0:unsafe.Sizeof(*ce)]
	if err := co.SendData(bufView); err != nil {
		panic(err)
	}
}
