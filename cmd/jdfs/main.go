// Command jdfs runs as the Just Data FileSystem server daemon for a specified fs root
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/complyue/jdfs/pkg/jdfs"
	"github.com/golang/glog"
)

func init() {
	// change glog default destination to stderr
	if glog.V(0) { // should always be true, mention glog so it defines its flags before we change them
		if err := flag.CommandLine.Set("logtostderr", "true"); nil != err {
			log.Printf("Failed changing glog default desitination, err: %s", err)
		}
	}
}

var (
	tcpAddr string
)

func init() {
	flag.StringVar(&tcpAddr, "tcp", "0.0.0.0:1112", "`addr` specifies the TCP address for JDFS service")
}

func main() {
	flag.Usage = func() {
		fmt.Fprint(flag.CommandLine.Output(), `
This is JDFS Server, all options:

`)
		flag.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(), `
Simple usage:

 %s [ -tcp <service-addr> ] [ -ppc <parallelism> ] <export-root>

`, os.Args[0])
	}
	flag.Parse()

	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(1)
	}

	sharedRoot := flag.Args()[0]
	absRoot, err := filepath.Abs(sharedRoot)
	if err != nil {
		fmt.Printf("Error with [%s] as root to share: +%v", sharedRoot, err)
		os.Exit(2)
	}

	if err = jdfs.ExportTCP(absRoot, tcpAddr); err != nil {
		fmt.Printf("Error serving JDFS root [%s]=>[%s]: +%v", sharedRoot, absRoot, err)
		os.Exit(3)
	}

}
