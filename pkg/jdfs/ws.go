package jdfs

import (
	"fmt"
	"os"

	"github.com/golang/glog"
)

// workset management methods

// MakeWorksetRoot exclusively creates a new unnique workset root dir under `baseDir`,
// with name resembling `nameHint`.
//
// name of baseDir should start with '.' to have workset files hidden from public
// data file lookups.
func (efs *exportedFileSystem) MakeWorksetRoot(baseDir, nameHint string) {
	co := efs.ho.Co()
	// release wire during working
	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	wsrd, errReason := "", ""
	// finally send result back
	defer func() {
		if err := co.StartSend(); err != nil {
			panic(err)
		}
		if err := co.SendObj(fmt.Sprintf("%#v", errReason)); err != nil {
			panic(err)
		}
		if err := co.SendObj(fmt.Sprintf("%#v", wsrd)); err != nil {
			panic(err)
		}
	}()

	// validate baseDir
	if len(baseDir) <= 1 || baseDir[0] != '.' {
		errReason = fmt.Sprintf("invalid base dir [%s] for workset", baseDir)
		return
	}
	// ensure the baseDir dir
	if err := os.MkdirAll(baseDir, 0755); err != nil && !os.IsExist(err) {
		errReason = fmt.Sprintf("can not create workset base dir [%s] - %+v", baseDir, err)
		return
	}
	// exclusively create a workset root dir, append a seq number as necessary
	wsrd = fmt.Sprintf("%s/%s", baseDir, nameHint)
	seq := 1
	for ; seq <= 50000; seq++ {
		if err := os.Mkdir(wsrd, 0755); err == nil {
			return
		} else if !os.IsExist(err) {
			errReason = fmt.Sprintf("unexpected error making workset dir [%s] - %+v",
				wsrd, err)
			return
		}
		wsrd = fmt.Sprintf("%s/%s-%d", baseDir, nameHint, seq)
	}
	errReason = fmt.Sprintf("so many (%d) worksets under name [%s]$[%s] ?!",
		seq-1, baseDir, nameHint)
}

// DiscardWorksetRoot removes a workset root dir for cleanup
func (efs *exportedFileSystem) DiscardWorksetRoot(wsrd string) {
	co := efs.ho.Co()
	// release wire during working
	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	if len(wsrd) <= 1 || wsrd[0] != '.' {
		glog.Errorf("WS not removing malformed workset root dir [%s]", wsrd)
	}
	if err := os.RemoveAll(wsrd); err != nil {
		glog.Errorf("WS failed removing workset root dir [%s] - %+v", wsrd, err)
	}
}

// CommitWorkset moves specified persistent data files under the workset root dir to
// overwrite public data files at same path.
//
// todo support for 2 phase commit ?
func (efs *exportedFileSystem) CommitWorkset(wsrd string, nFiles int,
	metaExt, dataExt string) {
	co := efs.ho.Co()

	pubPathList := make([]string, nFiles)
	for i := 0; i < nFiles; i++ {
		if pubPath, err := co.RecvObj(); err != nil {
			panic(err)
		} else {
			pubPathList[i] = pubPath.(string)
		}
	}

	// release wire during working
	if err := co.FinishRecv(); err != nil {
		panic(err)
	}

	errReason := ""

	// finally send result back
	defer func() {
		if err := co.StartSend(); err != nil {
			panic(err)
		}
		if err := co.SendObj(fmt.Sprintf("%#v", errReason)); err != nil {
			panic(err)
		}
	}()

	// validate wsrd
	if len(wsrd) <= 1 || wsrd[0] != '.' {
		glog.Error("WS not comitting malformed workset root dir [%s]", wsrd)
		errReason = "bad wsrd"
		return
	}

	// todo currently it's a best-effort commit and prone to partial errors during the commit.
	//      consider jdfs node scoped workset lock, make use of ZFS snapshot to implement
	//      atomic recovery from commit failures. note it might be mandatory for jdfsRootPath
	//      to be a ZFS filesystem root for free of collision with the snapshot mechanism.
	for _, pubPath := range pubPathList {
		privPath := wsrd + "/" + pubPath
		if err := os.Rename(privPath+metaExt, pubPath+metaExt); err != nil {
			errReason = fmt.Sprintf("Failed committing meta file [%s]", pubPath)
			return
		}
		if err := os.Rename(privPath+dataExt, pubPath+dataExt); err != nil {
			errReason = fmt.Sprintf("Failed committing data file [%s]", pubPath)
			return
		}
	}
}

// process work dir `wd` for commit of the workset identified by the root dir `wsrd`
func commitFiles(wsrd, wd string) {
	// Note: pwd is jdfsRootPath, all paths to underlying fs should be relative,
	// so as to be against jdfsRootPath.
	wsd := wsrd
	if len(wd) > 0 {
		wsd = wsrd + "/" + wd
	}
	df, err := os.OpenFile(wsd, os.O_RDONLY, 0)
	if err != nil {
		glog.Warningf("WS failed open workset dir [%s]:[%s] - %+v", jdfsRootPath, wsd, err)
		return
	}
	defer df.Close() // hold an ancestor dir open during recursion within it
	childFIs, err := df.Readdir(0)
	if err != nil {
		glog.Errorf("WS failed reading workset dir [%s]:[%s] - %+v", jdfsRootPath, wsd, err)
		return
	}
	for _, childFI := range childFIs {
		fn := childFI.Name()
		if childFI.IsDir() {
			// a dir
			pubDir := fn
			if len(wd) > 0 {
				pubDir = wd + "/" + fn
			}
			os.MkdirAll(pubDir, 0755)
			commitFiles(wsrd, pubDir)
		} else if childFI.Mode().IsRegular() {
			// a regular file
			pubPath := fn
			if len(wd) > 0 {
				pubPath = wd + "/" + fn
			}
			privPath := wsd + "/" + fn
			if err := os.Rename(privPath, pubPath); err != nil {
				// TODO fail the whole commit, atomatically
				glog.Errorf("WS failed committing workset file [%s]:[%s]$[%s] - %+v",
					jdfsRootPath, wsrd, pubPath, err)
			}
		} else {
			// a file not reigned by JDFS
			glog.Warningf("WS not committing file in workset [%s]:[%s]$[%s/%s]",
				jdfsRootPath, wsrd, wd, fn)
			continue
		}
	}
}
