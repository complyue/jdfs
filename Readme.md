# JDFS

**Just Data FileSystem** - **JDFS** is a
[networked](https://en.wikipedia.org/wiki/Computer_network)
[userspace filesystem](https://en.wikipedia.org/wiki/Filesystem_in_Userspace)
with responsibilities (such as
[access control](https://en.wikipedia.org/wiki/Access_control)
) those beyond upright data availability & consistency, offloaded.

Simply deployed alone (1 JDFS server <=> n JDFS clients), JDFS seeks to replace
[NFS](https://en.wikipedia.org/wiki/Network_File_System)
in many
[HPC](https://en.wikipedia.org/wiki/High-performance_computing)
scenarios where
[it sucks](https://www.kernel.org/doc/ols/2006/ols2006v2-pages-59-72.pdf)
.

But the main purpose of JDFS is to contribute data focused, performance-critical
parts (i.e. components at various granularity, with server and client the most
coarse ones) into analytical solutions (e.g. a homegrown
[array database](https://en.wikipedia.org/wiki/Array_DBMS)
), with ease.

> In my opinion, what’s going to happen over the next five years is that
> everyone is going to move from business intelligence to data science,
> and this data will be a sea change from what I’ll call stupid analytics,
> to what I’ll call smart analytics, which is correlations, data clustering,
> predictive modeling, data mining, Bayes classification.
>
> All of these words mean complex analytics. All that stuff is defined on
> arrays, and none of it is in SQL. So the world will move to smart analytics
> from stupid analytics, and that’s where we are.

—— Michael Stonebraker
[2014](https://www.datanami.com/2014/04/09/array_databases_the_next_big_thing_in_data_analytics_/)


JDFS server is stateful, in contrast to NFS, a JDFS server process basically
proxies all file operations on behalf of the JDFS client, i.e. keep files
open, locked, mmap'ed and synced, and etc.

All server side states, including resource occupation from os perspective,
will be naturally freed/released by means of that the JDFS server process,
just exits, once the underlying JDFS connection is disconnected.

If the disconnection is unexpected by the very JDFS client, it should fail
all pending fs operations, and discard all cached data as well, at the client
side.

The client can choose to fail hard by unmounting the client fs, or decide
to keep the mounted fs under certain circumstances, and reconnect to JDFS
server. In this case it can tell client applications accessing the mounted
JDFS to try again.

But any new connection is treated by the JDFS server as a fresh new mount,
such that a fresh server process is started serving each incoming JDFS
connection.
