package publisher

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
	nodeType       uint32
	id             uint64
	key            []byte
	brokerChildren []uint64
}

// AddChainPath takes a map of slices of child nodes and builds a more detailed
// collection of nodes to use in the Chain algorithm.
// It takes as input a map of slices of child nodes (first index is the node
// and the slice is a list of children of that node).
func (p *Publisher) AddChainPath(chainPath map[string][]string) {
	localNodeStr := "P" + strconv.FormatUint(p.localID, 10)

	// Build the nodes
	for nodeStr, childrenStr := range chainPath {

		if nodeStr == localNodeStr {
			tempNode := chainNode{}
			tempNode.nodeType = PublisherEnum
			tempNode.id = p.localID
			// This is the publisher. Don't need to add a key to itself.
			tempNode.addChildren(childrenStr)
			p.chainNodes[nodeStr] = tempNode
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
			tempNode.key = p.brokers[tempNode.id].key
			tempNode.addChildren(childrenStr)
			p.chainNodes[nodeStr] = tempNode
		} else if strings.HasPrefix(nodeStr, "S") {
			// Do nothing. The publisher should not know about the subscribers.
		}

	}

	fmt.Printf("%v\n", p.chainNodes)
}

// addChildren adds the child nodes. It takes as input a slice of child strings.
func (n *chainNode) addChildren(children []string) {
	for _, child := range children {
		if strings.HasPrefix(child, "B") {
			id, err := strconv.ParseUint(child[1:], 10, 64)
			if err != nil {
				fmt.Printf("Error parsing %v.\n", child)
				continue
			}
			n.brokerChildren = append(n.brokerChildren, id)
		}
	}
}

// handleChainPublish processes a Bracha's Reliable Broadcast publish.
// It takes as input a publication.
func (p *Publisher) handleChainPublish(pub *pb.Publication) {
	fromStr := "P" + strconv.FormatUint(p.localID, 10)

	// For this node's  broker children
	for _, childID := range p.chainNodes[fromStr].brokerChildren {
		// Need to make a new Publication just in case sending to
		// multiple nodes.
		tempPub := &pb.Publication{
			PubType:       pub.PubType,
			PublisherID:   pub.PublisherID,
			PublicationID: pub.PublicationID,
			TopicID:       pub.TopicID,
			BrokerID:      pub.BrokerID,
			Contents:      pub.Contents,
		}

		childStr := "B" + strconv.FormatUint(childID, 10)

		p.addMACs(tempPub, fromStr, childStr, chainRange)

		// Send the publication to that child.
		p.brokersMutex.RLock()
		if p.brokers[childID].toCh != nil {
			select {
			case p.brokers[childID].toCh <- *tempPub:
				fmt.Printf("%v\n", tempPub)
			}
		}
		p.brokersMutex.RUnlock()
	}
}

func (p *Publisher) addMACs(pub *pb.Publication, fromStr string, nodeStr string, generations int) {
	if generations > 1 {
		// Add MACs for all the children
		for _, childID := range p.chainNodes[nodeStr].brokerChildren {
			childStr := "B" + strconv.FormatUint(childID, 10)
			chainMAC := pb.ChainMAC{
				From: fromStr,
				To:   childStr,
				MAC:  common.CreatePublicationMAC(pub, p.chainNodes[childStr].key, common.Algorithm),
			}

			pub.ChainMACs = append(pub.ChainMACs, &chainMAC)

			// Recursively add child macs for next generation
			p.addMACs(pub, fromStr, childStr, generations-1)
		}
	}
}
