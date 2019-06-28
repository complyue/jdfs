package jdfc

import (
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/complyue/jdfs/pkg/errors"
	"github.com/complyue/jdfs/pkg/fuse"
	"github.com/golang/glog"
)

// PrepareMountpoint takes an argument for mountpoint, determines the absolute path for the
// mountpoint, validate it for JDFS mounting, including to detect if it's a stale FUSE and
// try unmount it if so.
func PrepareMountpoint(mpArg string) (mountpoint string, err error) {
	mountpoint, err = filepath.Abs(mpArg)
	if err != nil {
		err = errors.Wrapf(err, "Error resolving mountpoint path [%s]", mpArg)
		return
	}
	// broken fuse mountpoint will fail to stat on Linux but not on macOS,
	// readdir will fail on both, so ls the mountpoint to determine whether there's
	// a previous fuse mount broken (jdfc crashed most prolly).
	var df *os.File
	df, err = os.OpenFile(mountpoint, os.O_RDONLY, 0)
	if err != nil {
		glog.Warningf("Try unmounting [%s] as it appears not accessible ...", mountpoint)
		// try unmount a broken fuse mount first, or this jdfc can not succeed mounting.
		if err = fuse.Unmount(mountpoint); err == nil {
			if df, err = os.OpenFile(mountpoint, os.O_RDONLY, 0); err == nil {
				var names []string
				if names, err = df.Readdirnames(0); err != nil {
					err = errors.Wrapf(err, "Can not ls mountpoint [%s]", mountpoint)
					return
				} else if len(names) > 0 {
					glog.V(1).Infof("Mounting on non-empty dir [%s] with %d children.", mountpoint, len(names))
				}
			}
		}
	}
	if err != nil {
		err = errors.Wrapf(err, "Can not read mountpoint [%s]", mountpoint)
		return
	}
	defer df.Close()

	return
}

// ResolveJDFS infers JDFS server information from specified url and target mountpoint.
func ResolveJDFS(urlArg, mountpoint string) (jdfsURL *url.URL,
	jdfsHost, jdfsPath string, err error) {
	var jdfsHostName, jdfsPort string
	defer func() {
		if len(jdfsHostName) <= 0 {
			jdfsURL = nil
			return
		}

		if len(jdfsPort) <= 0 {
			jdfsPort = "1112"
		}
		jdfsHost = jdfsHostName + ":" + jdfsPort

		if strings.HasPrefix(jdfsPath, "/") {
			jdfsPath = jdfsPath[1:] // make sure jdfsPath is always relative
		}

		if jdfsURL == nil {
			jdfsURL = &url.URL{
				Scheme: "jdfs",
				Host:   jdfsHost,
				Path:   jdfsPath,
			}
		}
	}()

	if len(urlArg) > 0 {
		// jdfs url specified
		jdfsURL, err = url.Parse(urlArg)
		if err != nil {
			err = errors.Wrapf(err, "Failed parsing jdfs url [%s]", urlArg)
			return
		}
		if !jdfsURL.IsAbs() || "jdfs" != jdfsURL.Scheme {
			err = errors.Errorf("Invalid jdfs url: [%s]", urlArg)
		}
		jdfsHostName = jdfsURL.Hostname()
		jdfsPort = jdfsURL.Port()
		if len(jdfsURL.Path) <= 0 || jdfsURL.Path == "/" {
			jdfsPath = ""
		} else {
			jdfsPath = jdfsURL.Path
		}
		return
	}

	// jdfs url not specified, find a magic file for root url
	for atDir := mountpoint; ; {
		magicFn := filepath.Join(atDir, "__jdfs_root__")
		var mfi os.FileInfo
		if mfi, err = os.Stat(magicFn); err == nil {
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
			var jdfsRootURL *url.URL
			if jdfsRootURL, err = url.Parse(magicRoot); err != nil {
				err = errors.Wrapf(err, "Failed parsing JDFS root from file [%s] > [%s]", magicFn, magicRoot)
				return
			}

			if !jdfsRootURL.IsAbs() || "jdfs" != jdfsRootURL.Scheme {
				err = errors.Errorf("Invalid JDFS url: [%s] in [%s]", magicRoot, magicFn)
				return
			}
			jdfsHostName = jdfsRootURL.Hostname()
			jdfsPort = jdfsRootURL.Port()

			var mpRel string
			if mpRel, err = filepath.Rel(atDir, mountpoint); err != nil {
				err = errors.Errorf("Can not determine relative path from [%s] to [%s]", atDir, mountpoint)
				return
			}
			if mpRel == "." {
				mpRel = ""
			}

			jdfsRootPath := jdfsRootURL.Path
			if len(jdfsRootPath) <= 0 || jdfsRootPath == "/" {
				jdfsRootPath = ""
			}

			glog.V(1).Infof("Using relative path [%s] appended to root jdfs url [%s] configured in [%s]",
				mpRel, magicRoot, magicFn)
			if len(jdfsRootPath) <= 0 {
				jdfsPath = mpRel
			} else {
				jdfsPath = filepath.Join(jdfsRootPath, mpRel)
			}

			// inherite query/fragment from configured root url
			derivedURL := *jdfsRootURL
			derivedURL.Path = jdfsPath
			jdfsURL = &derivedURL

			break
		}
		upDir := filepath.Dir(atDir)
		if upDir == atDir {
			// reached root
			break
		}
		atDir = upDir
	}

	return
}
