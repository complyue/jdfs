// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fuse

import (
	// vfs was separated from this package (fuse) to be dependable by jdfs while
	// still buildable for smartos (a.k.a. illumos, solaris), as fuse only support
	// linux and osx (a.k.a. darwin) atm.
	// import all artifacts back here to minimize changes in this package for the
	// separation.
	. "github.com/complyue/jdfs/pkg/vfs"
)

// A sentinel used for unknown ops. The user is expected to respond with a
// non-nil error.
type unknownOp struct {
	OpCode uint32
	Inode  InodeID
}

// Causes us to cancel the associated context.
type interruptOp struct {
	FuseID uint64
}

// Required in order to mount on Linux and OS X.
type initOp struct {
	// In
	Kernel Protocol

	// In/out
	Flags InitFlags

	// Out
	Library      Protocol
	MaxReadahead uint32
	MaxWrite     uint32
}

type DestroyOp struct {
	// destroy
}
