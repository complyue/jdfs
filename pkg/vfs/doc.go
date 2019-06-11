// Package vfs defines virtual filesystem data structures shared among JDFS server and client
//
// vfs is separated from fuse package because fuse+vfs=>jdfc can only support linux and osx atm,
// while vfs=>jdfs needs to support smartos in addition. its inappropriate to stuff fuse with
// smartos (a.k.a. illumos, solaris) specific artifacts for now, but without that, jdfs can not
// be built for smartos if it depends on fuse.
package vfs
