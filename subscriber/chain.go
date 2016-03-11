package subscriber

import (
	pb "github.com/ericnorway/arbitraryFailures/proto"
)

// How far ahead and back to look in the chain
var chainRange = 2

// Enumeration of node types
const (
	PublisherEnum = iota
	BrokerEnum
	SubscriberEnum
)

type chainNode struct {
	nodeType           uint32
	id                 uint64
	key                []byte
	brokerChildren     []uint64
	subscriberChildren []uint64
}

// handleChainPublication processes a Chain publication.
// It takes as input a publication.
func (s *Subscriber) handleChainPublication(pub *pb.Publication) bool {
	macsValid := false

	return macsValid
}
