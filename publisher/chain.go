package publisher

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
}

// AddChainPath takes a map of slices of child nodes and builds a more detailed
// collection of nodes to use in the Chain algorithm.
// It takes as input a map of slices of child nodes (first index is the node
// and the slice is a list of children of that node).
func (p *Publisher) AddChainPath(chainPath map[string][]string) {

	// Build the nodes
	for nodeStr, childrenStr := range chainPath {
		tempNode := chainNode{}

		id, err := strconv.ParseUint(nodeStr[1:], 10, 64)
		if err != nil {
			fmt.Printf("Error parsing %v.\n", nodeStr)
			continue
		}

		if nodeStr == p.localStr {
			// This is the publisher. Don't need to add a key to itself.
			tempNode.addChildren(childrenStr)
		} else if strings.HasPrefix(nodeStr, "P") {
			continue
		} else if strings.HasPrefix(nodeStr, "B") {
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
// It takes as input a publication. It returns a status for the publication
// as output.
func (p *Publisher) handleChainPublish(pub *pb.Publication) pb.PubResponse_Status {

	// For this node's broker children, should be only one
	for _, childStr := range p.chainNodes[p.localStr].children {
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

		p.addMACs(tempPub, childStr, p.chainRange)

		// Send the publication to that child.
		p.brokersMutex.RLock()
		if p.brokers[childID].toCh != nil {
			select {
			case p.brokers[childID].toCh <- *tempPub:
			}
		}
		p.brokersMutex.RUnlock()

		select {
		case status := <-p.statusCh:
			return pb.PubResponse_Status(status)
		}
	}

	return -1
}

// addMACsRecursive adds MACs to the chain of MACs.
// It takes as input the publication,
// the current node ID string in the tree (should start with the local node's children),
// and the number of generations to add.
func (p *Publisher) addMACs(pub *pb.Publication, currentStr string, generations uint64) {
	if generations > 1 {
		// Add MACs for all the children
		for _, childStr := range p.chainNodes[currentStr].children {
			chainMAC := pb.ChainMAC{
				From: p.localStr,
				To:   childStr,
				MAC:  common.CreatePublicationMAC(pub, p.chainNodes[childStr].key, common.Algorithm),
			}

			pub.ChainMACs = append(pub.ChainMACs, &chainMAC)

			// Recursively add child macs for next generation
			p.addMACs(pub, childStr, generations-1)
		}
	}
}
