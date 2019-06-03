// Package jdfc defines the implementation of Just Data FileSystem client
package jdfc

import (
	"fmt"
	"net/url"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/complyue/hbi"
	"github.com/complyue/hbi/interop"
)

type DataFileServerConnector func(he *hbi.HostingEnv) (
	po *hbi.PostingEnd, ho *hbi.HostingEnd, err error,
)

func ServeDataFiles(
	jdfURL url.URL,
	jdfsConnector DataFileServerConnector,
	mountpoint string,
) error {
	he := hbi.NewHostingEnv()

	// expose names for interop
	interop.ExposeInterOpValues(he)

	po, ho, err := jdfsConnector(he)
	if err != nil {
		return err
	}
	defer ho.Close()

	c, err := fuse.Mount(
		mountpoint,
		fuse.FSName(jdfURL.String()),
		fuse.Subtype("jdf"),
		fuse.VolumeName(jdfURL.Fragment),

		//
		fuse.DefaultPermissions(),
		fuse.NoAppleDouble(),
		fuse.NoAppleXattr(),
		fuse.ExclCreate(),

		//
		fuse.ReadOnly(),
		//
		fuse.AsyncRead(),
		fuse.WritebackCache(),
	)
	if err != nil {
		return err
	}
	defer c.Close()

	if p := c.Protocol(); !p.HasInvalidate() {
		return fmt.Errorf("kernel FUSE support is too old to have invalidations: version %v", p)
	}

	filesys := &DataFileSystem{
		rootINode: 1,
	}
	if err = fs.Serve(c, filesys); err != nil {
		return err
	}

	// Check if the mount process has an error to report.
	<-c.Ready
	if err := c.MountError; err != nil {
		return err
	}

	po.Disconnect("", false)

	select {
	case <-po.Done():
		// hbic disconnected
	}

	return nil
}

type DataFileSystem struct {
	//
	rootINode fuse.NodeID
}

func (fs *DataFileSystem) Root() (fs.Node, error) {
	return nil, nil
}
