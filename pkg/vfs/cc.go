package vfs

import "flag"

// cache control

// CacheValidSeconds specifies the time for FUSE kernel cache to be valid, in seconds.
var CacheValidSeconds uint64 = 10

func init() {
	flag.Uint64Var(&CacheValidSeconds, "fuse-cache", 10, "FUSE cache valid time in `seconds`")
}
