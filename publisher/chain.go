package publisher

import (
	"fmt"
	"strconv"
	"strings"

	//pb "github.com/ericnorway/arbitraryFailures/proto"
)

// How far ahead and back to look in the chain
var chainRange = 2

// Enumeration of node types
const (
	PublisherEnum = iota
	BrokerEnum
	SubscriberEnum
)

type chainLink struct {
	linkType uint32
	id       uint64
	key      []byte
}

// AddChainPath takes slice of slices of nodes and builds a more detailed
// collection of nodes to use in the Chain algorithm.
// It takes as input a slice of slices of nodes (first index is position in
// the path and the second index references all the nodes in that position)
// and the local ID.
func (p *Publisher) AddChainPath(chainPath [][]string, id uint64) {
	position := -1
	thisNode := fmt.Sprintf("PUBLISHER%v", id)

	// Find the position of the local ID in the chain path.
	for i, currentNodes := range chainPath {
		for _, node := range currentNodes {
			if node == thisNode {
				position = i
			}
		}
	}

	if position == -1 {
		return
	}

	for i, currentNodes := range chainPath {
		// If the link is outside the range, skip it
		if i < position-chainRange || i > position+chainRange {
			continue
		}

		for _, node := range currentNodes {
			var link chainLink

			// Build the link
			if strings.HasPrefix(node, "PUBLISHER") {
				// Do nothing
			} else if strings.HasPrefix(node, "BROKER") {
				link.linkType = BrokerEnum
				idStr := strings.TrimPrefix(node, "BROKER")
				tmpID, err := strconv.ParseUint(idStr, 10, 64)
				link.id = tmpID
				if err != nil {
					fmt.Printf("%v\n", err)
					continue
				}
				link.key = p.brokers[link.id].key
			} else if strings.HasPrefix(node, "SUBSCRIBER") {
				// Do nothing
			}

			// Add the link to the correct position.
			if i == position-2 {
				p.chainLinks[-2] = append(p.chainLinks[-2], link)
			} else if i == position-1 {
				p.chainLinks[-1] = append(p.chainLinks[-1], link)
			} else if i == position+1 {
				p.chainLinks[1] = append(p.chainLinks[1], link)
			} else if i == position+2 {
				p.chainLinks[2] = append(p.chainLinks[2], link)
			}
		}
	}

	fmt.Printf("%v\n", p.chainLinks)
}
