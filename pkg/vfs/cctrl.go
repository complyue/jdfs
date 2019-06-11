package vfs

import "time"

// cache control

const (
	// todo make these configurable by some means

	META_ATTRS_CACHE_TIME   = 500 * time.Millisecond
	DIR_CHILDREN_CACHE_TIME = 1000 * time.Millisecond
)
