package subscriber

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ericnorway/arbitraryFailures/common"
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
	nodeType      uint32
	id            uint64
	key           []byte
	brokerParents []uint64
}

// AddChainPath takes a map of slices of child nodes and builds a more detailed
// collection of nodes to use in the Chain algorithm.
// It takes as input a map of slices of parent nodes (first index is the node
// and the slice is a list of parents of that node).
func (s *Subscriber) AddChainPath(rChainPath map[string][]string) {
	localNodeStr := "S" + strconv.FormatUint(s.localID, 10)

	// Build the nodes
	for nodeStr, parentsStr := range rChainPath {

		if strings.HasPrefix(nodeStr, "P") {
			// Do nothing. The subscriber should not know about the publishers.
		} else if strings.HasPrefix(nodeStr, "B") {
			tempNode := chainNode{}
			tempNode.nodeType = BrokerEnum
			idStr := strings.TrimPrefix(nodeStr, "B")
			tmpID, err := strconv.ParseUint(idStr, 10, 64)
			tempNode.id = tmpID
			if err != nil {
				fmt.Printf("%v\n", err)
				continue
			}
			tempNode.key = s.brokers[tempNode.id].key
			tempNode.addParents(parentsStr)
			s.chainNodes[nodeStr] = tempNode
		} else if nodeStr == localNodeStr {
			tempNode := chainNode{}
			tempNode.nodeType = SubscriberEnum
			tempNode.id = s.localID
			// This is the publisher. Don't need to add a key to itself.
			tempNode.addParents(parentsStr)
			s.chainNodes[nodeStr] = tempNode
		}
	}

	fmt.Printf("%v\n\n", s.chainNodes)
}

// addParents adds the parent nodes. It takes as input a slice of parent strings.
func (n *chainNode) addParents(parents []string) {
	for _, parent := range parents {
		if strings.HasPrefix(parent, "B") {
			id, err := strconv.ParseUint(parent[1:], 10, 64)
			if err != nil {
				fmt.Printf("Error parsing %v.\n", parent)
				continue
			}
			n.brokerParents = append(n.brokerParents, id)
		}
	}
}

// handleChainPublication processes an Authenticated Broadcast publication.
// It takes as input a publication.
func (s *Subscriber) handleChainPublication(pub *pb.Publication) bool {
	macsVerified := false

	// Verify MACs
	macsVerified = true

	// Make the map so not trying to access nil reference
	if s.pubsLearned[pub.PublisherID] == nil {
		s.pubsLearned[pub.PublisherID] = make(map[int64]string)
	}
	// If already learned this publication
	if s.pubsLearned[pub.PublisherID][pub.PublicationID] != "" {
		// fmt.Printf("Already learned publication %v from publisher %v.\n", pub.PublicationID, pub.PublisherID)
		return false
	}

	s.pubsLearned[pub.PublisherID][pub.PublicationID] = common.GetInfo(pub)

	return macsVerified
}
