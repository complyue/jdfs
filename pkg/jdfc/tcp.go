package jdfc

import "github.com/complyue/hbi"

// ConnTCP connects a JDFS client to the JDFS server over a TCP socket
// dialed to serverAddr.
func ConnTCP(serverAddr string) func(he *hbi.HostingEnv) (
	po *hbi.PostingEnd, ho *hbi.HostingEnd, err error,
) {
	return func(he *hbi.HostingEnv) (
		po *hbi.PostingEnd, ho *hbi.HostingEnd, err error,
	) {
		return hbi.DialTCP(serverAddr, he)
	}
}
