package fuse

import (
	"fmt"

	// vfs was separated from this package (fuse) to be dependable by jdfs while
	// still buildable for smartos (a.k.a. illumos, solaris), as fuse only support
	// linux and osx (a.k.a. darwin) atm.
	// import all artifacts back here to minimize changes in this package for the
	// separation.
	. "github.com/complyue/jdfs/pkg/vfs"
)

// this was originally in simple_types.go which had been moved to vfs.
func init() {
	// Make sure the constant above is correct. We do this at runtime rather than
	// defining the constant in terms of RootID for two reasons:
	//
	//  1. Users can more clearly see that the root ID is low and can therefore
	//     be used as e.g. an array index, with space reserved up to the root.
	//
	//  2. The constant can be untyped and can therefore more easily be used as
	//     an array index.
	//
	if RootInodeID != RootID {
		panic(fmt.Sprintf(
			"Oops, RootInodeID is wrong: %v vs. %v",
			RootInodeID,
			RootID))
	}
}
