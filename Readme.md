# JDFS

**Just Data FileSystem** - **JDFS** is a
[networked](https://en.wikipedia.org/wiki/Computer_network)
[userspace filesystem](https://en.wikipedia.org/wiki/Filesystem_in_Userspace)
with responsibilities (such as
[access control](https://en.wikipedia.org/wiki/Access_control)
) those beyond upright data availability & consistency, offloaded. Its purpose
has a few implications, including:

- It's highly vulnerable if exposed to untrusted environments, it's necessary
  to implemented sufficient access control by other means, e.g.
  [SSH tunneling](https://www.ssh.com/ssh/tunneling/)
  or
  [VPN](https://en.wikipedia.org/wiki/Virtual_private_network).
- The performance is terrible at serving many small files as all meta data
  read/write must go roundtrip the network layer, bcoz FUSE kernel cache
  is not workable for networked filesystem.

Simply deployed alone (1 **jdfs** <=> n **jdfc**), JDFS seeks to replace
[NFS](https://en.wikipedia.org/wiki/Network_File_System)
in many
[HPC](https://en.wikipedia.org/wiki/High-performance_computing)
scenarios where
[it sucks](https://www.kernel.org/doc/ols/2006/ols2006v2-pages-59-72.pdf)
.

But the main purpose of JDFS is to contribute data focused, performance-critical
parts (i.e. components at various granularity, with **jdfs** - the service/server,
and **jdfc** - the consumer/client, the most coarse ones) into analytical solutions
(e.g. a homegrown
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

JDFS server is stateful, in contrast to NFS, a **jdfs** process basically
proxies all file operations on behalf of the **jdfc**:

- fsync
  - always mapped 1 to 1
- open/close
  - mapped 1 to 1 from **jdfc** on Linux
  - forged by osxfuse from **jdfc** on macOS
- read/write/mmap
  - forged by all FUSE kernels

Any new connection is treated by the **jdfs** as a fresh new mount,
a fresh server process is started to proxy all operations from the
connecting **jdfc**.

And all server side states, including resource occupation from os perspective,
will be naturally freed/released by means of that the **jdfs** process,
just exits, once the underlying JDFS connection is disconnected.

