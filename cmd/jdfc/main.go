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

	jdfHostName, jdfPort := "", ""
	jdfPath := "/"
	if len(urlArg) > 0 {
		// jdfs url specified on cmdl
		jdfsURL, err = url.Parse(urlArg)
		if err != nil {
			log.Fatalf("Failed parsing JDFS url [%s] - %+v", urlArg, err)
		}
		if jdfsURL.IsAbs() && "jdf" != jdfsURL.Scheme {
			log.Fatalf("Invalid JDFS url: [%s]", urlArg)
		}
		jdfHostName = jdfsURL.Hostname()
		jdfPort = jdfsURL.Port()
		if len(jdfsURL.Path) <= 0 {
			jdfPath = "/"
		} else {
			jdfPath = jdfsURL.Path
		}
	} else {
		// jdfs url not specified, find a magic file for root url
		for atDir := mpFullPath; ; {
			magicFn := filepath.Join(atDir, "__jdf_root__")
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
				if jdfRootURL, err := url.Parse(magicRoot); err != nil {
					log.Fatalf("Failed parsing JDFS root from file [%s] > [%s] - %+v", magicFn, magicRoot, err)
				} else {
					if jdfRootURL.IsAbs() && "jdf" != jdfRootURL.Scheme {
						log.Fatalf("Invalid JDFS url: [%s] in [%s]", magicRoot, magicFn)
					}
					jdfHostName = jdfRootURL.Hostname()
					jdfPort = jdfRootURL.Port()
					if mpRel, err := filepath.Rel(atDir, mpFullPath); err != nil {
						log.Fatalf("Can not determine relative path from [%s] to [%s]", atDir, mpFullPath)
					} else {
						glog.V(1).Infof("Using relative path [%s] appended to root JDFS url [%s] configured in [%s]", mpRel, magicRoot, magicFn)
						if len(jdfRootURL.Path) <= 0 {
							jdfPath = "/" + mpRel
						} else {
							jdfPath = filepath.Join(jdfRootURL.Path, mpRel)
						}
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

	if len(jdfHostName) <= 0 {
		flag.Usage()
		os.Exit(2)
	}

	if len(jdfPort) <= 0 {
		jdfPort = "1112"
	}
	jdfHost := jdfHostName + ":" + jdfPort

	if jdfsURL == nil {
		jdfsURL = &url.URL{
			Scheme: "jdf",
			Host:   jdfHost,
			Path:   jdfPath,
		}
	}
	readOnly := len(jdfsURL.Query().Get("ro")) > 0

	fsName := fmt.Sprintf("jdf://%s%s", jdfHost, jdfPath)

	cfg := &fuse.MountConfig{
		Subtype:  "jdf",
		FSName:   fsName,
		ReadOnly: readOnly,

		EnableVnodeCaching: true,

		ErrorLogger: log.New(os.Stderr, "jdfc: ", 0),
		// DisableWritebackCaching: true,

		Options: map[string]string{
			"nonempty": "", // allow mounting on to none empty dirs on linux
		},
	}

	if glog.V(3) {
		cfg.DebugLogger = log.New(os.Stderr, "jdfc: ", 0)
	}

	fmt.Fprintf(os.Stderr, "Mounting %s to %v ...\n", fsName, mpFullPath)

	if glog.V(1) {
		return
	}

	if err = jdfc.ServeDataFiles(jdfc.ConnTCP(jdfHost), mpFullPath, cfg); err != nil {
		log.Fatal(err)
	}
}
