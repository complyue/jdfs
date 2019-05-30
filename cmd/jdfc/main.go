// Command jdfc runs as the Just Data FileSystem client daemon for a specified mount point
package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/complyue/fuse"
	"github.com/complyue/fuse/fuseops"
	"github.com/complyue/fuse/fuseutil"
	"github.com/complyue/fuse/samples/flushfs"
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
		fmt.Fprintln(flag.CommandLine.Output(), `
This is JDFS Client, all options:
`)
		flag.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(), `
Simple usage:

 %s [ <jdfs-url> ] <mount-point>

`, os.Args[0])
	}
	flag.Parse()

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

	mpFullPath, err := filepath.Abs(mpArg)
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
		jdfsURL, err := url.Parse(urlArg)
		if err != nil {
			log.Fatalf("Failed parsing JDFS url [%s]", urlArg, err)
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

	fmt.Fprintf(os.Stderr, "Mounting jdf://%s%s to %v ...\n", jdfHost, jdfPath, mpFullPath)

	if glog.V(0) {
		return
	}

	cfg := &fuse.MountConfig{}

	if glog.V(3) {
		cfg.DebugLogger = log.New(os.Stderr, "jdfc: ", 0)
	}

	// Create the file system.
	server, err := flushfs.NewFileSystem(func(s string) error {
		glog.Infof("Flush: %s", s)
		return nil
	}, func(s string) error {
		glog.Infof("FSync: %s", s)
		return nil
	})
	if err != nil {
		panic(err)
	}

	mfs, err := fuse.Mount(mpFullPath, server, cfg)
	if err != nil {
		log.Fatalf("Mount: %v", err)
	}

	// Wait for it to be unmounted.
	if err = mfs.Join(context.Background()); err != nil {
		log.Fatalf("Join: %v", err)
	}
}

// Create a file system whose sole contents are a file named "foo" and a
// directory named "bar".
//
// The file may be opened for reading and/or writing. Its initial contents are
// empty. Whenever a flush or fsync is received, the supplied function will be
// called with the current contents of the file and its status returned.
//
// The directory cannot be modified.
func NewFileSystem(
	reportFlush func(string) error,
	reportFsync func(string) error) (server fuse.Server, err error) {
	fs := &flushFS{
		reportFlush: reportFlush,
		reportFsync: reportFsync,
	}

	server = fuseutil.NewFileSystemServer(fs)
	return
}

const (
	fooID = fuseops.RootInodeID + 1 + iota
	barID
)

type flushFS struct {
	fuseutil.NotImplementedFileSystem

	reportFlush func(string) error
	reportFsync func(string) error

	mu          sync.Mutex
	fooContents []byte // GUARDED_BY(mu)
}

////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////

// LOCKS_REQUIRED(fs.mu)
func (fs *flushFS) rootAttributes() fuseops.InodeAttributes {
	return fuseops.InodeAttributes{
		Nlink: 1,
		Mode:  0777 | os.ModeDir,
	}
}

// LOCKS_REQUIRED(fs.mu)
func (fs *flushFS) fooAttributes() fuseops.InodeAttributes {
	return fuseops.InodeAttributes{
		Nlink: 1,
		Mode:  0777,
		Size:  uint64(len(fs.fooContents)),
	}
}

// LOCKS_REQUIRED(fs.mu)
func (fs *flushFS) barAttributes() fuseops.InodeAttributes {
	return fuseops.InodeAttributes{
		Nlink: 1,
		Mode:  0777 | os.ModeDir,
	}
}

// LOCKS_REQUIRED(fs.mu)
func (fs *flushFS) getAttributes(id fuseops.InodeID) (
	attrs fuseops.InodeAttributes,
	err error) {
	switch id {
	case fuseops.RootInodeID:
		attrs = fs.rootAttributes()
		return

	case fooID:
		attrs = fs.fooAttributes()
		return

	case barID:
		attrs = fs.barAttributes()
		return

	default:
		err = fuse.ENOENT
		return
	}
}

////////////////////////////////////////////////////////////////////////
// FileSystem methods
////////////////////////////////////////////////////////////////////////

func (fs *flushFS) StatFS(
	ctx context.Context,
	op *fuseops.StatFSOp) (err error) {
	return
}

func (fs *flushFS) LookUpInode(
	ctx context.Context,
	op *fuseops.LookUpInodeOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Sanity check.
	if op.Parent != fuseops.RootInodeID {
		err = fuse.ENOENT
		return
	}

	// Set up the entry.
	switch op.Name {
	case "foo":
		op.Entry = fuseops.ChildInodeEntry{
			Child:      fooID,
			Attributes: fs.fooAttributes(),
		}

	case "bar":
		op.Entry = fuseops.ChildInodeEntry{
			Child:      barID,
			Attributes: fs.barAttributes(),
		}

	default:
		err = fuse.ENOENT
		return
	}

	return
}

func (fs *flushFS) GetInodeAttributes(
	ctx context.Context,
	op *fuseops.GetInodeAttributesOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	op.Attributes, err = fs.getAttributes(op.Inode)
	return
}

func (fs *flushFS) SetInodeAttributes(
	ctx context.Context,
	op *fuseops.SetInodeAttributesOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Ignore any changes and simply return existing attributes.
	op.Attributes, err = fs.getAttributes(op.Inode)

	return
}

func (fs *flushFS) OpenFile(
	ctx context.Context,
	op *fuseops.OpenFileOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Sanity check.
	if op.Inode != fooID {
		err = fuse.ENOSYS
		return
	}

	return
}

func (fs *flushFS) ReadFile(
	ctx context.Context,
	op *fuseops.ReadFileOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Ensure the offset is in range.
	if op.Offset > int64(len(fs.fooContents)) {
		return
	}

	// Read what we can.
	op.BytesRead = copy(op.Dst, fs.fooContents[op.Offset:])

	return
}

func (fs *flushFS) WriteFile(
	ctx context.Context,
	op *fuseops.WriteFileOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Ensure that the contents slice is long enough.
	newLen := int(op.Offset) + len(op.Data)
	if len(fs.fooContents) < newLen {
		padding := make([]byte, newLen-len(fs.fooContents))
		fs.fooContents = append(fs.fooContents, padding...)
	}

	// Copy in the data.
	n := copy(fs.fooContents[op.Offset:], op.Data)

	// Sanity check.
	if n != len(op.Data) {
		panic(fmt.Sprintf("Unexpected short copy: %v", n))
	}

	return
}

func (fs *flushFS) SyncFile(
	ctx context.Context,
	op *fuseops.SyncFileOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	err = fs.reportFsync(string(fs.fooContents))
	return
}

func (fs *flushFS) FlushFile(
	ctx context.Context,
	op *fuseops.FlushFileOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	err = fs.reportFlush(string(fs.fooContents))
	return
}

func (fs *flushFS) OpenDir(
	ctx context.Context,
	op *fuseops.OpenDirOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Sanity check.
	switch op.Inode {
	case fuseops.RootInodeID:
	case barID:

	default:
		err = fuse.ENOENT
		return
	}

	return
}

func (fs *flushFS) ReadDir(
	ctx context.Context,
	op *fuseops.ReadDirOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Create the appropriate listing.
	var dirents []fuseutil.Dirent

	switch op.Inode {
	case fuseops.RootInodeID:
		dirents = []fuseutil.Dirent{
			fuseutil.Dirent{
				Offset: 1,
				Inode:  fooID,
				Name:   "foo",
				Type:   fuseutil.DT_File,
			},

			fuseutil.Dirent{
				Offset: 2,
				Inode:  barID,
				Name:   "bar",
				Type:   fuseutil.DT_Directory,
			},
		}

	case barID:

	default:
		err = fmt.Errorf("Unexpected inode: %v", op.Inode)
		return
	}

	// If the offset is for the end of the listing, we're done. Otherwise we
	// expect it to be for the start.
	switch op.Offset {
	case fuseops.DirOffset(len(dirents)):
		return

	case 0:

	default:
		err = fmt.Errorf("Unexpected offset: %v", op.Offset)
		return
	}

	// Fill in the listing.
	for _, de := range dirents {
		n := fuseutil.WriteDirent(op.Dst[op.BytesRead:], de)

		// We don't support doing this in anything more than one shot.
		if n == 0 {
			err = fmt.Errorf("Couldn't fit listing in %v bytes", len(op.Dst))
			return
		}

		op.BytesRead += n
	}

	return
}
