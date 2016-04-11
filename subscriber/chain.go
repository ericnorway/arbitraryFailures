package subscriber

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ericnorway/arbitraryFailures/common"
	pb "github.com/ericnorway/arbitraryFailures/proto"
)

type chainNode struct {
	key     []byte
	parents []string
}

// AddChainPath takes a map of slices of child nodes and builds a more detailed
// collection of nodes to use in the Chain algorithm.
// It takes as input a map of slices of parent nodes (first index is the node
// and the slice is a list of parents of that node).
func (s *Subscriber) AddChainPath(rChainPath map[string][]string) {

	// Build the nodes
	for nodeStr, parentsStr := range rChainPath {
		tempNode := chainNode{}

		id, err := strconv.ParseUint(nodeStr[1:], 10, 64)
		if err != nil {
			fmt.Printf("Error parsing %v.\n", nodeStr)
			continue
		}

		if nodeStr == s.localStr {
			// This is the publisher. Don't need to add a key to itself.
			tempNode.addParents(parentsStr)
		} else if strings.HasPrefix(nodeStr, "P") {
			continue
		} else if strings.HasPrefix(nodeStr, "B") {
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

	// Verify MACs
	verified := s.verifyChainMACs(pub, s.chainRange)
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
	//fmt.Printf("Learned publication %v from publisher %v.\n", pub.PublicationID, pub.PublisherID)

	return true
}

// verifyChainMACs verifies the MACs for the Chain algorithm.
// It returns true if the MACs in the chain are verified.
// It takes as input the publication, and the number of generations to check.
func (s *Subscriber) verifyChainMACs(pub *pb.Publication, generations uint64) bool {

	// Nothing to check, no parents
	if len(s.chainNodes[s.localStr].parents) == 0 {
		return true
	}

	if generations > 0 {
		for _, parentStr := range s.chainNodes[s.localStr].parents {
			// Check the previous generation
			verified := s.verifyChainMACsRecursive(pub, parentStr, generations-1)
			if verified {
				return true
			}
		}
	}

	// Not all the MACs in the chain were verified.
	return false
}

// verifyChainMACsRecursive verifies the MACs for the Chain algorithm.
// It returns true if the MACs in the chain are verified.
// It takes as input the publication,
// the current node ID string in the tree (should start with the local node),
// and the number of generations to check.
func (s *Subscriber) verifyChainMACsRecursive(pub *pb.Publication, currentStr string, generations uint64) bool {

	// Nothing to check, no parents
	if len(s.chainNodes[currentStr].parents) == 0 {
		return true
	}

	// If still in the chain range
	if generations > 0 {
		foundMatch := false

		// Look through this node's parents and in the list of Chain MACs
		// for a matching set of Tos and Froms
		for _, parentStr := range s.chainNodes[currentStr].parents {
			for _, chainMAC := range pub.ChainMACs {
				if chainMAC.To == s.localStr && chainMAC.From == parentStr {
					foundMatch = true

					// Actually check the MAC here.
					if common.CheckPublicationMAC(pub, chainMAC.MAC, s.chainNodes[parentStr].key, common.Algorithm) == false {
						//fmt.Printf("***BAD MAC: Chain*** %v\n", *pub)
						return false
					}

					// Go back one more generation
					verified := s.verifyChainMACsRecursive(pub, parentStr, generations-1)
					if verified {
						return true
					}
				}
			}
		}

		// If couldn't find a MAC for this generation.
		if foundMatch == false {
			//fmt.Printf("***MISSING MAC: Chain*** %v\n", *pub)
			return false
		}
	} else {
		// No longer in the chain range, so all the MACs matched.
		return true
	}

	// Not all the MACs in the chain were verified.
	return false
}
