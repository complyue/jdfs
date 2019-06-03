package jdfc

import "github.com/complyue/hbi"

func ConnTCP(serverAddr string) DataFileServerConnector {
	return func(he *hbi.HostingEnv) (
		po *hbi.PostingEnd, ho *hbi.HostingEnd, err error,
	) {
		return hbi.DialTCP(serverAddr, he)
	}
}
