package vfs

import "time"

// cache control

const (
	// todo make these configurable by some means

	// The FUSE VFS layer in the kernel maintains a cache of file attributes,
	// used whenever up to date information about size, mode, etc. is needed.
	//
	// For example, this is the abridged call chain for fstat(2):
	//
	//  *  (http://goo.gl/tKBH1p) fstat calls vfs_fstat.
	//  *  (http://goo.gl/3HeITq) vfs_fstat eventuall calls vfs_getattr_nosec.
	//  *  (http://goo.gl/DccFQr) vfs_getattr_nosec calls i_op->getattr.
	//  *  (http://goo.gl/dpKkst) fuse_getattr calls fuse_update_attributes.
	//  *  (http://goo.gl/yNlqPw) fuse_update_attributes uses the values in the
	//     struct inode if allowed, otherwise calling out to the user-space code.
	//
	// In addition to obvious cases like fstat, this is also used in more subtle
	// cases like updating size information before seeking (http://goo.gl/2nnMFa)
	// or reading (http://goo.gl/FQSWs8).
	//
	// Most 'real' file systems do not set inode_operations::getattr, and
	// therefore vfs_getattr_nosec calls generic_fillattr which simply grabs the
	// information from the inode struct. This makes sense because these file
	// systems cannot spontaneously change; all modifications go through the
	// kernel which can update the inode struct as appropriate.
	//
	// In contrast, a FUSE file system may have spontaneous changes, so it calls
	// out to user space to fetch attributes. However this is expensive, so the
	// FUSE layer in the kernel caches the attributes if requested.
	//
	// This field controls when the attributes returned in this response and
	// stashed in the struct inode should be re-queried. Leave at the zero value
	// to disable caching.
	//
	// More reading:
	//     http://stackoverflow.com/q/21540315/1505451
	META_ATTRS_CACHE_TIME = 500 * time.Millisecond

	// The time until which the kernel may maintain an entry for this name to
	// inode mapping in its dentry cache. After this time, it will revalidate the
	// dentry.
	//
	// As in the discussion of attribute caching above, unlike real file systems,
	// FUSE file systems may spontaneously change their name -> inode mapping.
	// Therefore the FUSE VFS layer uses dentry_operations::d_revalidate
	// (http://goo.gl/dVea0h) to intercept lookups and revalidate by calling the
	// user-space LookUpInode method. However the latter may be slow, so it
	// caches the entries until the time defined by this field.
	//
	// Example code walk:
	//
	//     * (http://goo.gl/M2G3tO) lookup_dcache calls d_revalidate if enabled.
	//     * (http://goo.gl/ef0Elu) fuse_dentry_revalidate just uses the dentry's
	//     inode if fuse_dentry_time(entry) hasn't passed. Otherwise it sends a
	//     lookup request.
	//
	// Leave at the zero value to disable caching.
	//
	// Beware: this value is ignored on OS X, where entry caching is disabled by
	// default. See notes on MountConfig.EnableVnodeCaching for more.
	DIR_CHILDREN_CACHE_TIME = 1000 * time.Millisecond
)
