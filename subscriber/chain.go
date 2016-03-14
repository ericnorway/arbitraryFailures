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
	thisNodeStr := "S" + strconv.FormatUint(s.localID, 10)

	// Verify MACs
	verified := s.verifyChainMACs(pub, thisNodeStr, thisNodeStr, s.chainRange, true)
	if !verified {
		// fmt.Printf("Not verified\n")
		return false
	}
	// fmt.Printf("Verified\n")

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

	return true
}

// verifyChainMACs verifies the
// It takes the request as input.
func (s *Subscriber) verifyChainMACs(pub *pb.Publication, toStr string, nodeStr string, generations int, passThisGeneration bool) bool {
	// Nothing to check
	if len(s.chainNodes[nodeStr].parents) == 0 {
		return true
	}

	// Ignore the first generation. MAC is checked earlier.
	if passThisGeneration {
		for _, parentStr := range s.chainNodes[nodeStr].parents {
			// Check the previous generation
			verified := s.verifyChainMACs(pub, toStr, parentStr, generations-1, false)
			if verified {
				return true
			}
		}
		return false
	}

	// If still in the chain range
	if generations > 0 {
		// Look through this node's parents and in the list of Chain MACs
		// for a matching set of Tos and Froms
		for _, parentStr := range s.chainNodes[nodeStr].parents {
			for _, chainMAC := range pub.ChainMACs {
				if chainMAC.To == toStr && chainMAC.From == parentStr {

					// Actually check the MAC here.
					if common.CheckPublicationMAC(pub, chainMAC.MAC, s.chainNodes[parentStr].key, common.Algorithm) == false {
						fmt.Printf("***BAD MAC: Chain*** %v\n", *pub)
						return false
					}

					// Go back one more generation
					verified := s.verifyChainMACs(pub, toStr, parentStr, generations-1, false)
					if verified {
						return true
					}
				}
			}
		}
	} else {
		// No longer in the chain range, so all the MACs matched.
		return true
	}

	// Not all the MACs in the chain were verified.
	return false
}
