package publisher

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
}

// AddChainPath takes a map of slices of child nodes and builds a more detailed
// collection of nodes to use in the Chain algorithm.
// It takes as input a map of slices of child nodes (first index is the node
// and the slice is a list of children of that node).
func (p *Publisher) AddChainPath(chainPath map[string][]string) {
	localNodeStr := "P" + strconv.FormatUint(p.localID, 10)

	// Build the nodes
	for nodeStr, childrenStr := range chainPath {
		tempNode := chainNode{}

		id, err := strconv.ParseUint(nodeStr[1:], 10, 64)
		if err != nil {
			fmt.Printf("Error parsing %v.\n", nodeStr)
			continue
		}

		if nodeStr == localNodeStr {
			tempNode.idStr = nodeStr
			// This is the publisher. Don't need to add a key to itself.
			tempNode.addChildren(childrenStr)
		} else if strings.HasPrefix(nodeStr, "P") {
			continue
		} else if strings.HasPrefix(nodeStr, "B") {
			tempNode.idStr = nodeStr
			tempNode.key = p.brokers[id].key
			tempNode.addChildren(childrenStr)
		} else if strings.HasPrefix(nodeStr, "S") {
			continue
		} else {
			continue
		}

		p.chainNodes[nodeStr] = tempNode
	}

	// fmt.Printf("%v\n", p.chainNodes)
}

// addChildren adds the child nodes. It takes as input a slice of child strings.
func (n *chainNode) addChildren(children []string) {
	for _, child := range children {
		n.children = append(n.children, child)
	}
}

// handleChainPublish processes a Bracha's Reliable Broadcast publish.
// It takes as input a publication.
func (p *Publisher) handleChainPublish(pub *pb.Publication) {
	fromStr := "P" + strconv.FormatUint(p.localID, 10)

	// For this node's  broker children
	for _, childStr := range p.chainNodes[fromStr].children {
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
			BrokerID:      pub.BrokerID,
			Contents:      pub.Contents,
		}

		p.addMACs(tempPub, fromStr, childStr, p.chainRange)

		// Send the publication to that child.
		p.brokersMutex.RLock()
		if p.brokers[childID].toCh != nil {
			select {
			case p.brokers[childID].toCh <- *tempPub:
			}
		}
		p.brokersMutex.RUnlock()
	}
}

func (p *Publisher) addMACs(pub *pb.Publication, fromStr string, nodeStr string, generations int) {
	if generations > 1 {
		// Add MACs for all the children
		for _, childStr := range p.chainNodes[nodeStr].children {
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
