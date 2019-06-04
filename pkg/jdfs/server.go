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
	exportRoot string

	po *hbi.PostingEnd
	ho *hbi.HostingEnd

	jdfsUID, jdfsGID int

	readonly   bool
	rootDir    *os.File
	rootDevice int32
	rootInode  fuse.InodeID
}

func (efs *exportedFileSystem) NamesToExpose() []string {
	return []string{
		"Mount",
	}
}

func (efs *exportedFileSystem) Mount(readonly bool, jdfsPath string) {
	efs.jdfsUID = os.Geteuid()
	efs.jdfsGID = os.Getegid()

	efs.readonly = readonly

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
		efs.rootDevice = rootStat.Dev
		efs.rootInode = fuse.InodeID(rootStat.Ino)

		co := efs.ho.Co()
		if err := co.StartSend(); err != nil {
			panic(err)
		}
		co.SendCode(fmt.Sprintf(`
Mounted(%#v, %#v, %#v)
`, efs.rootInode, efs.jdfsUID, efs.jdfsGID))
	} else {
		// todo inspect fs type etc.
		efs.ho.Disconnect(fmt.Sprintf("Incompatible local filesystem at JDFS server path: [%s]=>[%s]",
			jdfsPath, exportPath), true)
		return
	}

}
