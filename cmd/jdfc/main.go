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
	if fpi, err := os.Stat(mpFullPath); err != nil {
		if os.IsNotExist(err) {
			log.Fatalf("Mountpoint [%s] does not exists!", mpFullPath)
		} else {
			log.Fatalf("Can not stat mountpoint path [%s] - %+v", mpFullPath, err)
		}
	} else if !fpi.IsDir() {
		log.Fatalf("Mountpoint [%s] not a dir!", mpFullPath)
	}

	jdfsHostName, jdfsPort := "", ""
	jdfsPath := "/"
	if len(urlArg) > 0 {
		// jdfs url specified on cmdl
		jdfsURL, err = url.Parse(urlArg)
		if err != nil {
			log.Fatalf("Failed parsing JDFS url [%s] - %+v", urlArg, err)
		}
		if jdfsURL.IsAbs() && "jdfs" != jdfsURL.Scheme {
			log.Fatalf("Invalid JDFS url: [%s]", urlArg)
		}
		jdfsHostName = jdfsURL.Hostname()
		jdfsPort = jdfsURL.Port()
		if len(jdfsURL.Path) <= 0 {
			jdfsPath = "/"
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
						glog.V(1).Infof("Using relative path [%s] appended to root JDFS url [%s] configured in [%s]", mpRel, magicRoot, magicFn)
						if len(jdfsRootURL.Path) <= 0 {
							jdfsPath = "/" + mpRel
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
	jdfHost := jdfsHostName + ":" + jdfsPort
	fsName := fmt.Sprintf("jdfs://%s%s", jdfHost, jdfsPath)

	if jdfsURL == nil {
		jdfsURL = &url.URL{
			Scheme: "jdfs",
			Host:   jdfHost,
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
		Subtype:  "jdfs",
		FSName:   fsName,
		ReadOnly: readOnly,

		// caching should be okay once InvalidateNode/InvalidateEntry are implemented and
		// cache invalidated appropriately. Tracking: https://github.com/jacobsa/fuse/issues/64
		EnableVnodeCaching: true,

		ErrorLogger: log.New(os.Stderr, "jdfc: ", 0),

		Options: mntOpts,
	}

	if glog.V(3) {
		cfg.DebugLogger = log.New(os.Stderr, "jdfc: ", 0)
	}

	if err = jdfc.ServeDataFiles(jdfc.ConnTCP(jdfHost), mpFullPath, cfg); err != nil {
		log.Fatal(err)
	}
}
