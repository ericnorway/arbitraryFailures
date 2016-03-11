package broker

import (
	"fmt"
	"strconv"
	"strings"

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
	nodeType           uint32
	id                 uint64
	key                []byte
	brokerChildren     []uint64
	subscriberChildren []uint64
	brokerParents      []uint64
	publisherParents   []uint64
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

		if nodeStr == localNodeStr {
			tempNode.nodeType = PublisherEnum
			tempNode.id = b.localID
			// This is the local broker. Don't need to add a key to itself.
			tempNode.addChildren(childrenStr)
		} else if strings.HasPrefix(nodeStr, "P") {
			tempNode.nodeType = BrokerEnum
			idStr := strings.TrimPrefix(nodeStr, "P")
			tmpID, err := strconv.ParseUint(idStr, 10, 64)
			tempNode.id = tmpID
			if err != nil {
				fmt.Printf("%v\n", err)
				continue
			}
			tempNode.key = b.publishers[tempNode.id].key
			tempNode.addChildren(childrenStr)
		} else if strings.HasPrefix(nodeStr, "B") {
			tempNode.nodeType = BrokerEnum
			idStr := strings.TrimPrefix(nodeStr, "B")
			tmpID, err := strconv.ParseUint(idStr, 10, 64)
			tempNode.id = tmpID
			if err != nil {
				fmt.Printf("%v\n", err)
				continue
			}
			tempNode.key = b.remoteBrokers[tempNode.id].key
			tempNode.addChildren(childrenStr)
		} else if strings.HasPrefix(nodeStr, "S") {
			tempNode.nodeType = BrokerEnum
			idStr := strings.TrimPrefix(nodeStr, "S")
			tmpID, err := strconv.ParseUint(idStr, 10, 64)
			tempNode.id = tmpID
			if err != nil {
				fmt.Printf("%v\n", err)
				continue
			}
			tempNode.key = b.subscribers[tempNode.id].key
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

		if nodeStr == localNodeStr {
			tempNode.nodeType = PublisherEnum
			tempNode.id = b.localID
			// This is the local broker. Don't need to add a key to itself.
			tempNode.addParents(parentsStr)
		} else if strings.HasPrefix(nodeStr, "P") {
			tempNode.nodeType = BrokerEnum
			idStr := strings.TrimPrefix(nodeStr, "P")
			tmpID, err := strconv.ParseUint(idStr, 10, 64)
			tempNode.id = tmpID
			if err != nil {
				fmt.Printf("%v\n", err)
				continue
			}
			tempNode.key = b.publishers[tempNode.id].key
			// The publishers don't have children.
			// (visible to this collection of brokers that is).
			// Publisher might be another broker.
		} else if strings.HasPrefix(nodeStr, "B") {
			tempNode.nodeType = BrokerEnum
			idStr := strings.TrimPrefix(nodeStr, "B")
			tmpID, err := strconv.ParseUint(idStr, 10, 64)
			tempNode.id = tmpID
			if err != nil {
				fmt.Printf("%v\n", err)
				continue
			}
			tempNode.key = b.remoteBrokers[tempNode.id].key
			tempNode.addParents(parentsStr)
		} else if strings.HasPrefix(nodeStr, "S") {
			tempNode.nodeType = BrokerEnum
			idStr := strings.TrimPrefix(nodeStr, "S")
			tmpID, err := strconv.ParseUint(idStr, 10, 64)
			tempNode.id = tmpID
			if err != nil {
				fmt.Printf("%v\n", err)
				continue
			}
			tempNode.key = b.subscribers[tempNode.id].key
			tempNode.addParents(parentsStr)
		}

		b.chainNodes[nodeStr] = tempNode
	}

	fmt.Printf("%v\n\n", b.chainNodes)
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
		if strings.HasPrefix(child, "S") {
			id, err := strconv.ParseUint(child[1:], 10, 64)
			if err != nil {
				fmt.Printf("Error parsing %v.\n", child)
				continue
			}
			n.subscriberChildren = append(n.subscriberChildren, id)
		}
	}
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
		if strings.HasPrefix(parent, "P") {
			id, err := strconv.ParseUint(parent[1:], 10, 64)
			if err != nil {
				fmt.Printf("Error parsing %v.\n", parent)
				continue
			}
			n.publisherParents = append(n.publisherParents, id)
		}
	}
}

// handleAbPublish handles Authenticated Broadcast publish requests.
// It takes the request as input.
func (b Broker) handleChainPublish(pub *pb.Publication) {
	fmt.Printf("Publication: %v.\n", pub)
}
