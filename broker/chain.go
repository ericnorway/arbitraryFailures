package broker

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ericnorway/arbitraryFailures/common"
	pb "github.com/ericnorway/arbitraryFailures/proto"
)

type chainNode struct {
	key      []byte
	children []string
	parents  []string
}

// AddChainPath takes a map of slices of child nodes and builds a more detailed
// collection of nodes to use in the Chain algorithm.
// It takes as input a map of slices of child nodes (first index is the node
// and the slice is a list of children of that node),
// and a map of slices of parent nodes.
func (b *Broker) AddChainPath(chainPath map[string][]string, rChainPath map[string][]string) {

	// Build the nodes
	for nodeStr, childrenStr := range chainPath {
		tempNode, exists := b.chainNodes[nodeStr]

		if !exists {
			tempNode = chainNode{}
		}

		id, err := strconv.ParseUint(nodeStr[1:], 10, 64)
		if err != nil {
			fmt.Printf("Error parsing %v.\n", nodeStr)
			continue
		}

		if nodeStr == b.localStr {
			// This is the local broker. Don't need to add a key to itself.
			tempNode.addChildren(childrenStr)
		} else if strings.HasPrefix(nodeStr, "P") {
			tempNode.key = b.publishers[id].key
			tempNode.addChildren(childrenStr)
		} else if strings.HasPrefix(nodeStr, "B") {
			tempNode.key = b.remoteBrokers[id].key
			tempNode.addChildren(childrenStr)
		} else if strings.HasPrefix(nodeStr, "S") {
			tempNode.key = b.subscribers[id].key
			// The subscribers don't have children.
			// (visible to this collection of brokers that is).
			// Subscriber might be another broker.
		} else {
			continue
		}

		b.chainNodes[nodeStr] = tempNode
	}

	// Build the nodes
	for nodeStr, parentsStr := range rChainPath {
		tempNode, exists := b.chainNodes[nodeStr]

		if !exists {
			tempNode = chainNode{}
		}

		id, err := strconv.ParseUint(nodeStr[1:], 10, 64)
		if err != nil {
			fmt.Printf("Error parsing %v.\n", nodeStr)
			continue
		}

		if nodeStr == b.localStr {
			// This is the local broker. Don't need to add a key to itself.
			tempNode.addParents(parentsStr)
		} else if strings.HasPrefix(nodeStr, "P") {
			tempNode.key = b.publishers[id].key
			// The publishers don't have parents.
			// (visible to this collection of brokers that is).
			// Publisher might be another broker.
		} else if strings.HasPrefix(nodeStr, "B") {
			tempNode.key = b.remoteBrokers[id].key
			tempNode.addParents(parentsStr)
		} else if strings.HasPrefix(nodeStr, "S") {
			tempNode.key = b.subscribers[id].key
			tempNode.addParents(parentsStr)
		}

		b.chainNodes[nodeStr] = tempNode
	}

	// fmt.Printf("%v\n\n", b.chainNodes)
}

// addChildren adds the child nodes. It takes as input a slice of child strings.
func (n *chainNode) addChildren(children []string) {
	for _, child := range children {
		n.children = append(n.children, child)
	}
}

// addParents adds the parent nodes. It takes as input a slice of parent strings.
func (n *chainNode) addParents(parents []string) {
	for _, parent := range parents {
		n.parents = append(n.parents, parent)
	}
}

// handleAbPublish handles Authenticated Broadcast publish requests.
// It takes the request as input.
func (b *Broker) handleChainPublish(pub *pb.Publication) bool {
	// fmt.Printf("Handle Chain Publish Publication %v, Publisher %v, Broker %v.\n", pub.PublicationID, pub.PublisherID, pub.BrokerID)

	if b.chainSent[pub.PublisherID] == nil {
		b.chainSent[pub.PublisherID] = make(map[int64]bool)
	}

	// If this publication has already been sent
	if b.chainSent[pub.PublisherID][pub.PublicationID] == true {
		// fmt.Printf("Already sent Publication %v, Publisher %v, Broker %v.\n", pub.PublicationID, pub.PublisherID, pub.BrokerID)
		return false
	}

	verified := b.verifyChainMACs(pub, b.chainRange)
	if !verified {
		// fmt.Printf("Not verified\n")
		return false
	}
	// fmt.Printf("Verified\n")

	// For this node's children
	for _, childStr := range b.chainNodes[b.localStr].children {
		childID, err := strconv.ParseUint(childStr[1:], 10, 64)
		if err != nil {
			fmt.Printf("Error parsing %v.\n", childStr)
			continue
		}

		// Need to make a new Publication just in case sending to
		// multiple nodes.
		tempPub := &pb.Publication{
			PubType:       pub.PubType,
			PublisherID:   pub.PublisherID,
			PublicationID: pub.PublicationID,
			TopicID:       pub.TopicID,
			BrokerID:      b.localID,
			Contents:      pub.Contents,
		}

		b.addMACsRecursive(tempPub, pub, childStr, b.chainRange)

		if strings.HasPrefix(childStr, "B") {
			// Send the publication to that child.
			b.remoteBrokersMutex.RLock()
			if b.remoteBrokers[childID].toChainCh != nil {
				select {
				case b.remoteBrokers[childID].toChainCh <- *tempPub:
				}
			}
			b.remoteBrokersMutex.RUnlock()
		} else if strings.HasPrefix(childStr, "S") {
			// Send the publication to that child.
			b.subscribersMutex.RLock()
			if b.subscribers[childID].toCh != nil {
				select {
				case b.subscribers[childID].toCh <- *tempPub:
				}
			}
			b.subscribersMutex.RUnlock()
		}
	}

	// Mark this publication as sent
	b.chainSent[pub.PublisherID][pub.PublicationID] = true

	return true
}

// verifyChainMACs verifies the MACs for the Chain algorithm.
// It returns true if the MACs in the chain are verified.
// It takes as input the publication,
// and the number of generations to check.
func (b *Broker) verifyChainMACs(pub *pb.Publication, generations int) bool {

	// Nothing to check, no parents
	if len(b.chainNodes[b.localStr].parents) == 0 {
		return true
	}

	if generations > 0 {
		for _, parentStr := range b.chainNodes[b.localStr].parents {
			// Check the previous generation
			verified := b.verifyChainMACsRecursive(pub, parentStr, generations-1)
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
func (b *Broker) verifyChainMACsRecursive(pub *pb.Publication, currentStr string, generations int) bool {

	// Nothing to check, no parents
	if len(b.chainNodes[currentStr].parents) == 0 {
		return true
	}

	// If still in the chain range
	if generations > 0 {
		foundMatch := false

		// Look through this node's parents and in the list of Chain MACs
		// for a matching set of Tos and Froms
		for _, parentStr := range b.chainNodes[currentStr].parents {
			for _, chainMAC := range pub.ChainMACs {
				if chainMAC.To == b.localStr && chainMAC.From == parentStr {
					foundMatch = true

					// Actually check the MAC here.
					if common.CheckPublicationMAC(pub, chainMAC.MAC, b.chainNodes[parentStr].key, common.Algorithm) == false {
						fmt.Printf("***BAD MAC: Chain*** %v\n", *pub)
						return false
					}

					// Go back one more generation
					verified := b.verifyChainMACsRecursive(pub, parentStr, generations-1)
					if verified {
						return true
					}
				}
			}
		}

		// If couldn't find a MAC for this generation.
		if foundMatch == false {
			fmt.Printf("***MISSING MAC: Chain*** %v\n", *pub)
			return false
		}
	} else {
		// No longer in the chain range, so all the MACs matched.
		return true
	}

	// Not all the MACs in the chain were verified.
	return false
}

// addMACsRecursive adds MACs to the chain of MACs. Some older MACs may need to be kept depending
// on the number of generations in the chain.
// It takes as input the new publication,
// the old publication,
// the current node ID string in the tree (should start with the local node's children),
// and the number of generations to add.
func (b *Broker) addMACsRecursive(pub *pb.Publication, oldPub *pb.Publication, currentStr string, generations int) {
	// Add any old chain MACs that add going to the child node
	for _, oldChainMAC := range oldPub.ChainMACs {
		if oldChainMAC.To == currentStr {
			pub.ChainMACs = append(pub.ChainMACs, oldChainMAC)
		}
	}

	if generations > 1 {
		// Add MACs for all the broker children
		for _, childStr := range b.chainNodes[currentStr].children {
			chainMAC := pb.ChainMAC{
				From: b.localStr,
				To:   childStr,
				MAC:  common.CreatePublicationMAC(pub, b.chainNodes[childStr].key, common.Algorithm),
			}

			pub.ChainMACs = append(pub.ChainMACs, &chainMAC)

			// Recursively add child macs for next generation
			b.addMACsRecursive(pub, oldPub, childStr, generations-1)
		}
	}
}
