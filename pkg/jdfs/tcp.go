package jdfs

import (
	"flag"
	"fmt"
	"net"
	"os"

	"github.com/complyue/hbi"
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
		return newServiceEnv(exportRoot)
	}, func(listener *net.TCPListener) error {
		fmt.Fprintf(os.Stderr, "JDFS server %d for [%s] listening: %s\n",
			os.Getpid(), exportRoot, listener.Addr())
		return nil
	}); err != nil {
		return
	}

	return
}
