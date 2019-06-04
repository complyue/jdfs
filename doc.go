// Package jdfs defines the Go1 interface to communicate with
// JDFS server and client (local mounter).
//
// JDFS server is stateful, in contrast to NFS, a JDFS server process basically
// proxies all file operations on behalf of the JDFS client, i.e. keep files
// open, locked, mmap'ed and synced, and etc.
//
// All server side states, including resource occupation from os perspective,
// will be naturally freed/released by means of that the JDFS server process,
// just exits, once the underlying JDFS connection is disconnected.
//
// If the disconnection is unexpected by the very JDFS client, it should fail
// all pending fs operations, and discard all cached data as well, at the client
// side.
//
// The client can choose to fail hard by unmounting the client fs, or decide
// to keep the mounted fs under certain circumstances, and reconnect to JDFS
// server. In this case it can tell client applications accessing the mounted
// JDFS to try again.
//
// But any new connection is treated by the JDFS server as a fresh new mount,
// such that a fresh server process is started serving each incoming JDFS
// connection.
package jdfs
