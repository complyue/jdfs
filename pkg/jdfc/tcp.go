package jdfc

import "github.com/complyue/hbi"

// ConnTCP connects a jdf client to the jdf server over a tcp socket
// dialed to serverAddr.
func ConnTCP(serverAddr string) DataFileServerConnector {
	return func(he *hbi.HostingEnv) (
		po *hbi.PostingEnd, ho *hbi.HostingEnd, err error,
	) {
		return hbi.DialTCP(serverAddr, he)
	}
}
