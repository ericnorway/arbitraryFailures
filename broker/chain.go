package broker

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ericnorway/arbitraryFailures/common"
	pb "github.com/ericnorway/arbitraryFailures/proto"
)

type chainNode struct {
	idStr    string
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
	localNodeStr := "B" + strconv.FormatUint(b.localID, 10)

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

		if nodeStr == localNodeStr {
			tempNode.idStr = nodeStr
			// This is the local broker. Don't need to add a key to itself.
			tempNode.addChildren(childrenStr)
		} else if strings.HasPrefix(nodeStr, "P") {
			tempNode.idStr = nodeStr
			tempNode.key = b.publishers[id].key
			tempNode.addChildren(childrenStr)
		} else if strings.HasPrefix(nodeStr, "B") {
			tempNode.idStr = nodeStr
			tempNode.key = b.remoteBrokers[id].key
			tempNode.addChildren(childrenStr)
		} else if strings.HasPrefix(nodeStr, "S") {
			tempNode.idStr = nodeStr
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

		if nodeStr == localNodeStr {
			tempNode.idStr = nodeStr
			// This is the local broker. Don't need to add a key to itself.
			tempNode.addParents(parentsStr)
		} else if strings.HasPrefix(nodeStr, "P") {
			tempNode.idStr = nodeStr
			tempNode.key = b.publishers[id].key
			// The publishers don't have parents.
			// (visible to this collection of brokers that is).
			// Publisher might be another broker.
		} else if strings.HasPrefix(nodeStr, "B") {
			tempNode.idStr = nodeStr
			tempNode.key = b.remoteBrokers[id].key
			tempNode.addParents(parentsStr)
		} else if strings.HasPrefix(nodeStr, "S") {
			tempNode.idStr = nodeStr
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
func (b *Broker) handleChainPublish(pub *pb.Publication) {
	thisNodeStr := "B" + strconv.FormatUint(b.localID, 10)

	verified := b.verifyChainMACs(pub, thisNodeStr, thisNodeStr, common.ChainRange, true)
	if !verified {
		fmt.Printf("Not verified\n")
		return
	}
	fmt.Printf("Verified\n")

	// For this node's children
	for _, childStr := range b.chainNodes[thisNodeStr].children {
		childID, err := strconv.ParseUint(childStr[1:], 10, 64)
		if err != nil {
			fmt.Printf("Error parsing %v.\n", childID)
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

		b.addMACs(tempPub, pub, thisNodeStr, childStr, common.ChainRange)

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
}

// verifyChainMACs verifies the
// It takes the request as input.
func (b *Broker) verifyChainMACs(pub *pb.Publication, toStr string, nodeStr string, generations int, passThisGeneration bool) bool {
	// Nothing to check
	if len(b.chainNodes[nodeStr].parents) == 0 {
		return true
	}

	// Ignore the first generation. MAC is checked earlier.
	if passThisGeneration {
		for _, parentStr := range b.chainNodes[nodeStr].parents {
			// Check the previous generation
			verified := b.verifyChainMACs(pub, toStr, parentStr, generations-1, false)
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
		for _, parentStr := range b.chainNodes[nodeStr].parents {
			for _, chainMAC := range pub.ChainMACs {
				if chainMAC.To == toStr && chainMAC.From == parentStr {

					fmt.Printf("*   From: %v *\n", chainMAC.From)

					// Actually check the MAC here.
					if common.CheckPublicationMAC(pub, chainMAC.MAC, b.chainNodes[parentStr].key, common.Algorithm) == false {
						fmt.Printf("***BAD MAC: Chain*** %v\n", *pub)
						return false
					}

					// Go back one more generation
					verified := b.verifyChainMACs(pub, toStr, parentStr, generations-1, false)
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

func (b *Broker) addMACs(pub *pb.Publication, oldPub *pb.Publication, fromStr string, nodeStr string, generations int) {
	// Add any old chain MACs that add going to the child node
	for _, oldChainMAC := range oldPub.ChainMACs {
		if oldChainMAC.To == nodeStr {
			pub.ChainMACs = append(pub.ChainMACs, oldChainMAC)
		}
	}

	if generations > 1 {
		// Add MACs for all the broker children
		for _, childStr := range b.chainNodes[nodeStr].children {
			chainMAC := pb.ChainMAC{
				From: fromStr,
				To:   childStr,
				MAC:  common.CreatePublicationMAC(pub, b.chainNodes[childStr].key, common.Algorithm),
			}

			pub.ChainMACs = append(pub.ChainMACs, &chainMAC)

			// Recursively add child macs for next generation
			b.addMACs(pub, oldPub, fromStr, childStr, generations-1)
		}
	}
}
