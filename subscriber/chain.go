package subscriber

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ericnorway/arbitraryFailures/common"
	pb "github.com/ericnorway/arbitraryFailures/proto"
)

type chainNode struct {
	idStr   string
	key     []byte
	parents []string
}

// AddChainPath takes a map of slices of child nodes and builds a more detailed
// collection of nodes to use in the Chain algorithm.
// It takes as input a map of slices of parent nodes (first index is the node
// and the slice is a list of parents of that node).
func (s *Subscriber) AddChainPath(rChainPath map[string][]string) {
	localNodeStr := "S" + strconv.FormatUint(s.localID, 10)

	// Build the nodes
	for nodeStr, parentsStr := range rChainPath {
		tempNode := chainNode{}

		id, err := strconv.ParseUint(nodeStr[1:], 10, 64)
		if err != nil {
			fmt.Printf("Error parsing %v.\n", nodeStr)
			continue
		}

		if nodeStr == localNodeStr {
			tempNode.idStr = localNodeStr
			// This is the publisher. Don't need to add a key to itself.
			tempNode.addParents(parentsStr)
		} else if strings.HasPrefix(nodeStr, "P") {
			continue
		} else if strings.HasPrefix(nodeStr, "B") {
			tempNode.idStr = nodeStr
			tempNode.key = s.brokers[id].key
			tempNode.addParents(parentsStr)
		} else if strings.HasPrefix(nodeStr, "S") {
			continue
		} else {
			continue
		}

		s.chainNodes[nodeStr] = tempNode
	}

	// fmt.Printf("%v\n\n", s.chainNodes)
}

// addParents adds the parent nodes. It takes as input a slice of parent strings.
func (n *chainNode) addParents(parents []string) {
	for _, parent := range parents {
		n.parents = append(n.parents, parent)
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
	fmt.Printf("Learned publication %v from publisher %v.\n", pub.PublicationID, pub.PublisherID)

	return macsVerified
}
