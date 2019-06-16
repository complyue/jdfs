package jdfs

import (
	"flag"
	"fmt"
	"net"
	"os"

	"github.com/complyue/hbi"
	"github.com/complyue/hbi/interop"
	"github.com/complyue/hbi/mp"
)

var (
	soloMode bool
)

func init() {
	flag.BoolVar(&soloMode, "solo", false, "run jdfs in solo mode (no subprocess spawning) for easy debug")
}

// ExportTCP exports the specified root directory from local filesystem,
// with this dir and any sub directory under it (only if belongs to the same
// local filesystem) mountable as JDFS over TCP network, at the specified TCP
// service address.
func ExportTCP(exportRoot string, servAddr string) (err error) {

	servMethod := mp.UpstartTCP
	if soloMode { // should run in solo mode only for debug purpose
		servMethod = hbi.ServeTCP
	}

	if err = servMethod(servAddr, func() *hbi.HostingEnv {
		he := hbi.NewHostingEnv()

		interop.ExposeInterOpValues(he)

		he.ExposeFunction("__hbi_init__", // callback on wire connected
			func(po *hbi.PostingEnd, ho *hbi.HostingEnd) {
				efs := &exportedFileSystem{
					exportRoot: exportRoot,

					po: po, ho: ho,
				}

				he.ExposeReactor(efs)
			})

		return he
	}, func(listener *net.TCPListener) {
		fmt.Fprintf(os.Stderr, "JDFS server for [%s] listening: %s\n",
			exportRoot, listener.Addr())
	}); err != nil {
		return
	}

	return
}
