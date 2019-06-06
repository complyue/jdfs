// Package jdfs defines implementation of the Just Data FileSystem server
package jdfs

import (
	"fmt"
	"os"
	"syscall"

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

	// hold the JDFS mounted root dir open to prevent it unlinked before JDFS client disconnected
	rootDir *os.File

	// nested directory with other filesystems mounted will not be exported to JDFS client
	//
	// TODO detect and present nested mountpoints as readOnly, empty dirs to JDFS client,
	//      or support nested fs mounting over JDFS with FUSE generationNumber likely.
	rootDevice int64
	// JDFS client is not restricted to mount root of local filesystem of JDFS server,
	// in case a nested dir is exported to the JDFS client, JDFS mount root will have inode
	// other than 1, root inode translation will be perform at both sides, translating 1 to
	// this value.
	rootInode fuse.InodeID
}

func (efs *exportedFileSystem) NamesToExpose() []string {
	return []string{
		"Mount",
	}
}

func (efs *exportedFileSystem) Mount(readOnly bool, jdfsPath string) {
	efs.jdfsUID = os.Geteuid()
	efs.jdfsGID = os.Getegid()

	efs.readOnly = readOnly

	var exportPath string
	if jdfsPath == "/" || jdfsPath == "" {
		exportPath = efs.exportRoot
	} else {
		exportPath = efs.exportRoot + jdfsPath
	}

	rootDir, err := os.Open(exportPath)
	if err != nil {
		efs.ho.Disconnect(fmt.Sprintf("Bad JDFS server path: [%s]=>[%s]",
			jdfsPath, exportPath), true)
	}
	if fi, err := efs.rootDir.Stat(); err != nil || !fi.IsDir() {
		efs.ho.Disconnect(fmt.Sprintf("Invalid JDFS server path: [%s]=>[%s]",
			jdfsPath, exportPath), true)
		return
	} else if rootStat, ok := fi.Sys().(*syscall.Stat_t); ok {
		efs.rootDir = rootDir
		efs.rootDevice = int64(rootStat.Dev)
		efs.rootInode = fuse.InodeID(rootStat.Ino)

		co := efs.ho.Co()
		if err := co.StartSend(); err != nil {
			panic(err)
		}
		// send mount result fields
		if err := co.SendObj(hbi.Repr(hbi.LitListType{
			efs.rootInode, efs.jdfsUID, efs.jdfsGID,
		})); err != nil {
			panic(err)
		}
	} else {
		// todo inspect fs type etc.
		efs.ho.Disconnect(fmt.Sprintf("Incompatible local filesystem at JDFS server path: [%s]=>[%s]",
			jdfsPath, exportPath), true)
		return
	}

}
