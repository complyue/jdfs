// Command jdfc runs as the Just Data FileSystem client daemon for a specified mount point
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/complyue/jdfs/pkg/fuse"
	"github.com/complyue/jdfs/pkg/jdfc"
	"github.com/complyue/jdfs/pkg/vfs"

	"github.com/golang/glog"
)

func init() {
	// change glog default destination to stderr
	if glog.V(0) { // should always be true, mention glog so it defines its flags before we change them
		if err := flag.CommandLine.Set("logtostderr", "true"); nil != err {
			log.Printf("Failed changing glog default desitination, err: %s", err)
		}
	}
}

func main() {
	flag.Usage = func() {
		fmt.Fprint(flag.CommandLine.Output(), `
This is JDFS Client, all options:

`)
		flag.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(), `
Simple usage:

 %s [ <jdfs-url> ] <mount-point>

`, os.Args[0])
	}
	flag.Parse()

	var (
		mountpoint string
		err        error
	)

	urlArg, mpArg := "", ""
	switch flag.NArg() {
	case 2:
		cmdArgs := flag.Args()
		urlArg, mpArg = cmdArgs[0], cmdArgs[1]
	case 1:
		cmdArgs := flag.Args()
		mpArg = cmdArgs[0]
	default:
		flag.Usage()
		os.Exit(1)
	}

	mountpoint, err = jdfc.PrepareMountpoint(mpArg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%+v", err)
		os.Exit(5)
	}

	jdfsURL, jdfsHost, jdfsPath, err := jdfc.ResolveJDFS(urlArg, mountpoint)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%+v", err)
		os.Exit(5)
	}
	if jdfsURL == nil {
		flag.Usage()
		os.Exit(2)
	}

	readOnly := false
	mntOpts := map[string]string{
		"nonempty": "", // allow mounting on to none empty dirs on linux
	}
	for optKey, optVa := range jdfsURL.Query() {
		if optKey == "ro" {
			readOnly = true
		} else {
			// last value takes precedence if multiple present
			mntOpts[optKey] = optVa[len(optVa)-1]
		}
	}

	cfg := &fuse.MountConfig{
		Subtype:  "jdf",
		FSName:   jdfsURL.String(),
		ReadOnly: readOnly,

		// for macOS
		VolumeName: filepath.Base(mountpoint),

		ErrorLogger: log.New(os.Stderr, "jdfc: ", 0),

		Options: mntOpts,
	}

	if vfs.CacheValidSeconds > 0 {
		// caching should be okay as we are actively invalidating it when needed,
		// but explicitly setting it to zero can disable the cache
		cfg.EnableVnodeCaching = true
	}

	if glog.V(2) {
		cfg.DebugLogger = log.New(os.Stderr, "jdfc: ", 0)
	}

	if err = jdfc.MountJDFS(jdfc.ConnTCP(jdfsHost), jdfsPath, mountpoint, cfg); err != nil {
		log.Fatal(err)
	}
}
