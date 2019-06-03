// Package jdfc defines the implementation of Just Data FileSystem client
package jdfc

import (
	"context"
	"fmt"
	"log"

	"github.com/complyue/jdfs/pkg/fuse"
	"github.com/complyue/jdfs/pkg/fuseutil"

	"github.com/complyue/hbi"
	"github.com/complyue/hbi/interop"
)

type DataFileServerConnector func(he *hbi.HostingEnv) (
	po *hbi.PostingEnd, ho *hbi.HostingEnd, err error,
)

func ServeDataFiles(
	jdfsConnector DataFileServerConnector,
	mountpoint string,
	cfg *fuse.MountConfig,
) error {
	he := hbi.NewHostingEnv()

	// expose names for interop
	interop.ExposeInterOpValues(he)

	po, ho, err := jdfsConnector(he)
	if err != nil {
		return err
	}
	defer ho.Close()

	cfg.VolumeName = "JustDataFilesShared" // TODO allow config at server site and pull to here

	// Create the file system.
	server, err := NewFileSystem()
	if err != nil {
		panic(err)
	}

	mfs, err := fuse.Mount(mountpoint, server, cfg)
	if err != nil {
		log.Fatalf("Mount: %+v", err)
	}

	if p := mfs.Protocol(); !p.HasInvalidate() {
		return fmt.Errorf("kernel FUSE support is too old to have invalidations: version %v", p)
	}

	// Wait for it to be unmounted.
	if err = mfs.Join(context.Background()); err != nil {
		log.Fatalf("Join: %v", err)
	}

	po.Disconnect("", false)

	select {
	case <-po.Done():
		// hbic disconnected
	}

	return nil
}

func NewFileSystem() (server fuse.Server, err error) {
	fs := &fuseutil.NotImplementedFileSystem{}

	server = fuseutil.NewFileSystemServer(fs)
	return
}
