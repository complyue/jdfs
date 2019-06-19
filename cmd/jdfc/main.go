// Command jdfc runs as the Just Data FileSystem client daemon for a specified mount point
package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/complyue/jdfs/pkg/fuse"
	"github.com/complyue/jdfs/pkg/jdfc"

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
		mpFullPath string
		jdfsURL    *url.URL
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

	mpFullPath, err = filepath.Abs(mpArg)
	if err != nil {
		log.Fatalf("Error resolving mountpoint path [%s] - %+v", mpArg, err)
	}
	// broken fuse mountpoint will fail to stat on Linux but not on macOS,
	// readdir will fail on both, so ls the mountpoint to determine whether there's
	// a previous fuse mount broken (jdfc crashed most prolly).
	df, err := os.OpenFile(mpFullPath, os.O_RDONLY, 0)
	if err != nil {
		glog.Warningf("Try unmounting [%s] as it appears not accessible ...", mpFullPath)
		// try unmount a broken fuse mount first, or this jdfc can not succeed mounting.
		if err = fuse.Unmount(mpFullPath); err == nil {
			if df, err = os.OpenFile(mpFullPath, os.O_RDONLY, 0); err == nil {
				if names, err := df.Readdirnames(0); err != nil {
					log.Fatalf("Can not ls mountpoint path [%s] - %+v", mpFullPath, err)
					os.Exit(5)
				} else if len(names) > 0 {
					glog.V(1).Infof("Mounting on non-empty dir [%s] with %d children.", mpFullPath, len(names))
				}
			}
		}
	}
	if err != nil {
		log.Fatalf("Can not read mountpoint [%s] - %+v", mpFullPath, err)
		os.Exit(5)
	}
	defer df.Close()

	jdfsHostName, jdfsPort := "", ""
	jdfsPath := ""
	if len(urlArg) > 0 {
		// jdfs url specified on cmdl
		jdfsURL, err = url.Parse(urlArg)
		if err != nil {
			log.Fatalf("Failed parsing jdfs url [%s] - %+v", urlArg, err)
		}
		if jdfsURL.IsAbs() && "jdfs" != jdfsURL.Scheme {
			log.Fatalf("Invalid jdfs url: [%s]", urlArg)
		}
		jdfsHostName = jdfsURL.Hostname()
		jdfsPort = jdfsURL.Port()
		if len(jdfsURL.Path) <= 0 {
			jdfsPath = ""
		} else {
			jdfsPath = jdfsURL.Path
		}
	} else {
		// jdfs url not specified, find a magic file for root url
		for atDir := mpFullPath; ; {
			magicFn := filepath.Join(atDir, "__jdfs_root__")
			if mfi, err := os.Stat(magicFn); err == nil {
				if mfi.IsDir() {
					glog.Warningf("JDFS magic should not be a dir: [%s]", magicFn)
					continue
				}
				magicRoot := ""
				if magicFc, err := ioutil.ReadFile(magicFn); err != nil {
					glog.Warningf("Error reading JDFS magic [%s] - %+v", magicFn, err)
					continue
				} else {
					magicRoot = strings.TrimSpace(string(magicFc))
				}
				if jdfsRootURL, err := url.Parse(magicRoot); err != nil {
					log.Fatalf("Failed parsing JDFS root from file [%s] > [%s] - %+v", magicFn, magicRoot, err)
				} else {
					if jdfsRootURL.IsAbs() && "jdfs" != jdfsRootURL.Scheme {
						log.Fatalf("Invalid JDFS url: [%s] in [%s]", magicRoot, magicFn)
					}
					jdfsHostName = jdfsRootURL.Hostname()
					jdfsPort = jdfsRootURL.Port()
					if mpRel, err := filepath.Rel(atDir, mpFullPath); err != nil {
						log.Fatalf("Can not determine relative path from [%s] to [%s]", atDir, mpFullPath)
					} else {
						glog.V(1).Infof("Using relative path [%s] appended to root jdfs url [%s] configured in [%s]", mpRel, magicRoot, magicFn)
						if len(jdfsRootURL.Path) <= 0 {
							jdfsPath = mpRel
						} else {
							jdfsPath = filepath.Join(jdfsRootURL.Path, mpRel)
						}

						// inherite query/fragment from configured root url
						derivedURL := *jdfsRootURL
						derivedURL.Path = jdfsPath
						jdfsURL = &derivedURL
					}
					break
				}
			}
			upDir := filepath.Dir(atDir)
			if upDir == atDir {
				// reached root
				break
			}
			atDir = upDir
		}
	}

	if len(jdfsHostName) <= 0 {
		flag.Usage()
		os.Exit(2)
	}

	if len(jdfsPort) <= 0 {
		jdfsPort = "1112"
	}
	jdfsHost := jdfsHostName + ":" + jdfsPort

	if strings.HasPrefix(jdfsPath, "/") {
		jdfsPath = jdfsPath[1:] // make sure jdfsPath is always relative
	}

	fsName := fmt.Sprintf("jdfs://%s/%s", jdfsHost, jdfsPath)

	if jdfsURL == nil {
		jdfsURL = &url.URL{
			Scheme: "jdfs",
			Host:   jdfsHost,
			Path:   jdfsPath,
		}
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
		FSName:   fsName,
		ReadOnly: readOnly,

		// for macOS
		VolumeName: strings.NewReplacer("/", "~", ":", "#").Replace(
			fmt.Sprintf("%s%%%s", jdfsHost, jdfsPath),
		),

		ErrorLogger: log.New(os.Stderr, "jdfc: ", 0),

		Options: mntOpts,
	}

	if glog.V(2) {
		cfg.DebugLogger = log.New(os.Stderr, "jdfc: ", 0)
	}

	if err = jdfc.MountJDFS(jdfc.ConnTCP(jdfsHost), jdfsPath, mpFullPath, cfg); err != nil {
		log.Fatal(err)
	}
}
