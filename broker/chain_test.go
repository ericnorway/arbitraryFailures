package broker

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/ericnorway/arbitraryFailures/common"
	pb "github.com/ericnorway/arbitraryFailures/proto"
)

func TestHandleChainPublish(t *testing.T) {
	for i, test := range handleChainPublishTests {
		chain := make(map[string][]string)
		rChain := make(map[string][]string)

		// Add descendants
		for _, descendantStr := range test.descendants {
			temp := strings.Split(descendantStr, ":")
			node := temp[0]

			// If there are children to the node
			if len(temp) == 2 {
				children := strings.Split(temp[1], ",")
				if len(children) != 0 && children[0] != "" {
					chain[node] = children
				} else {
					chain[node] = nil
				}
			} else {
				chain[node] = nil
			}

			if node == test.thisNode {

			} else if strings.HasPrefix(node, "S") {
				id, err := strconv.ParseUint(node[1:], 10, 64)
				if err != nil {
					fmt.Printf("Error parsing %v.\n", node)
					continue
				}
				test.broker.AddSubscriber(id, []byte(""))
				test.broker.addToSubChannel(id)
				test.broker.subscribers[id].topics[1] = true
				test.broker.subscribers[id].topics[2] = true
				test.broker.subscribers[id].topics[3] = true
			} else if strings.HasPrefix(node, "B") {
				id, err := strconv.ParseUint(node[1:], 10, 64)
				if err != nil {
					fmt.Printf("Error parsing %v.\n", node)
					continue
				}
				test.broker.AddBroker(id, "", []byte(""))
				test.broker.addBrokerChannels(id)
			}
		}

		// Add ancestors
		for _, ancestorStr := range test.ancestors {
			temp := strings.Split(ancestorStr, ":")
			node := temp[0]

			// If there are parents to the node
			if len(temp) == 2 {
				parents := strings.Split(temp[1], ",")
				if len(parents) != 0 && parents[0] != "" {
					rChain[node] = parents
				} else {
					rChain[node] = nil
				}
			} else {
				rChain[node] = nil
			}

			if node == test.thisNode {

			} else if strings.HasPrefix(node, "P") {
				id, err := strconv.ParseUint(node[1:], 10, 64)
				if err != nil {
					fmt.Printf("Error parsing %v.\n", node)
					continue
				}
				test.broker.AddPublisher(id, []byte(""))
			} else if strings.HasPrefix(node, "B") {
				id, err := strconv.ParseUint(node[1:], 10, 64)
				if err != nil {
					fmt.Printf("Error parsing %v.\n", node)
					continue
				}
				test.broker.AddBroker(id, "", []byte(""))
				test.broker.addBrokerChannels(id)
			}
		}

		test.broker.chainRange = test.chainRange

		test.broker.AddChainPath(chain, rChain)

		// Add publication request
		sent := test.broker.handleChainPublish(&test.pubReq)
		if sent == false && test.wantPubs == true {
			t.Errorf("HandleAbPublish\ntest nr:%d\ndescription: %s\nwant: %v\ngot: %v\n",
				i+1, test.desc, &test.want, "Publication not sent.")
			continue
		}

		// Check that all "subscribers" got the forwarded publication
		for _, childStr := range test.broker.chainNodes[test.thisNode].children {

			id, err := strconv.ParseUint(childStr[1:], 10, 64)
			if err != nil {
				fmt.Printf("Error parsing %v.\n", childStr)
				continue
			}

			if strings.HasPrefix(childStr, "B") {
				broker := test.broker.remoteBrokers[id]
				select {
				case pub := <-broker.toChainCh:
					// Currently this is not checking each individual Chain MAC, only the length of ChainMACs slice.
					if !common.Equals(pub, test.want) {
						t.Errorf("HandleAbPublish\ntest nr:%d\ndescription: %s\nwant: %v\ngot: %v\n",
							i+1, test.desc, &test.want, &pub)
					}
				}
			} else if strings.HasPrefix(childStr, "S") {
				subscriber := test.broker.subscribers[id]
				select {
				case pub := <-subscriber.toCh:
					// Currently this is not checking each individual Chain MAC, only the length of ChainMACs slice.
					if !common.Equals(pub, test.want) {
						t.Errorf("HandleAbPublish\ntest nr:%d\ndescription: %s\nwant: %v\ngot: %v\n",
							i+1, test.desc, &test.want, &pub)
					}
				}
			}
		}
	}
}

var handleChainPublishTests = []struct {
	broker      *Broker
	desc        string
	thisNode    string
	chainRange  uint64
	ancestors   []string
	descendants []string
	pubReq      pb.Publication
	wantPubs    bool
	want        pb.Publication
}{
	{
		broker:     NewBroker(1, "localhost", 4, 0, 0),
		desc:       "Chain length 1, position B1",
		thisNode:   "B1",
		chainRange: 1,
		ancestors: []string{
			"S0:B2",
			"S1:B2",
			"S2:B2",
			"S3:B2",
			"B2:B1",
			"B1:B0",
			"B0:P0,P1,P2,P3",
			"P0",
			"P1",
			"P2",
			"P3",
		},
		descendants: []string{
			"P0:B0",
			"P1:B0",
			"P2:B0",
			"B0:B1",
			"B1:B2",
			"B2:S0,S1,S2,S3",
			"S0",
			"S1",
			"S2",
			"S3",
		},
		pubReq: pb.Publication{
			PubType:       common.Chain,
			PublisherID:   1,
			PublicationID: 1,
			TopicID:       1,
			Contents: [][]byte{
				[]byte(common.Message1),
			},
			ChainMACs: []*pb.ChainMAC{},
		},
		wantPubs: true,
		want: pb.Publication{
			PubType:       common.Chain,
			PublisherID:   1,
			PublicationID: 1,
			TopicID:       1,
			BrokerID:      1,
			Contents: [][]byte{
				[]byte(common.Message1),
			},
			ChainMACs: []*pb.ChainMAC{},
		},
	},
	{
		broker:     NewBroker(2, "localhost", 4, 0, 0),
		desc:       "Chain length 1, position B2",
		thisNode:   "B2",
		chainRange: 1,
		ancestors: []string{
			"S0:B2",
			"S1:B2",
			"S2:B2",
			"S3:B2",
			"B2:B1",
			"B1:B0",
			"B0:P0,P1,P2,P3",
			"P0",
			"P1",
			"P2",
			"P3",
		},
		descendants: []string{
			"P0:B0",
			"P1:B0",
			"P2:B0",
			"B0:B1",
			"B1:B2",
			"B2:S0,S1,S2,S3",
			"S0",
			"S1",
			"S2",
			"S3",
		},
		pubReq: pb.Publication{
			PubType:       common.Chain,
			PublisherID:   1,
			PublicationID: 1,
			TopicID:       1,
			Contents: [][]byte{
				[]byte(common.Message1),
			},
			ChainMACs: []*pb.ChainMAC{},
		},
		wantPubs: true,
		want: pb.Publication{
			PubType:       common.Chain,
			PublisherID:   1,
			PublicationID: 1,
			TopicID:       1,
			BrokerID:      2,
			Contents: [][]byte{
				[]byte(common.Message1),
			},
			ChainMACs: []*pb.ChainMAC{},
		},
	},
	{
		broker:     NewBroker(1, "localhost", 4, 0, 0),
		desc:       "Chain length 2, position B1",
		thisNode:   "B1",
		chainRange: 2,
		ancestors: []string{
			"S0:B2",
			"S1:B2",
			"S2:B2",
			"S3:B2",
			"B2:B1",
			"B1:B0",
			"B0:P0,P1,P2,P3",
			"P0",
			"P1",
			"P2",
			"P3",
		},
		descendants: []string{
			"P0:B0",
			"P1:B0",
			"P2:B0",
			"B0:B1",
			"B1:B2",
			"B2:S0,S1,S2,S3",
			"S0",
			"S1",
			"S2",
			"S3",
		},
		pubReq: pb.Publication{
			PubType:       common.Chain,
			PublisherID:   1,
			PublicationID: 1,
			TopicID:       1,
			Contents: [][]byte{
				[]byte(common.Message1),
			},
			ChainMACs: []*pb.ChainMAC{
				&pb.ChainMAC{
					From: "P1",
					To:   "B1",
					MAC:  []byte{16, 237, 212, 97, 95, 176, 56, 90, 125, 103, 39, 3, 52, 140, 47, 207},
				},
			},
		},
		wantPubs: true,
		want: pb.Publication{
			PubType:       common.Chain,
			PublisherID:   1,
			PublicationID: 1,
			TopicID:       1,
			BrokerID:      1,
			Contents: [][]byte{
				[]byte(common.Message1),
			},
			ChainMACs: []*pb.ChainMAC{
				&pb.ChainMAC{
					From: "B1",
					To:   "S0",
					MAC:  []byte{16, 237, 212, 97, 95, 176, 56, 90, 125, 103, 39, 3, 52, 140, 47, 207},
				},
				&pb.ChainMAC{
					From: "B1",
					To:   "S1",
					MAC:  []byte{16, 237, 212, 97, 95, 176, 56, 90, 125, 103, 39, 3, 52, 140, 47, 207},
				},
				&pb.ChainMAC{
					From: "B1",
					To:   "S2",
					MAC:  []byte{16, 237, 212, 97, 95, 176, 56, 90, 125, 103, 39, 3, 52, 140, 47, 207},
				},
				&pb.ChainMAC{
					From: "B1",
					To:   "S3",
					MAC:  []byte{16, 237, 212, 97, 95, 176, 56, 90, 125, 103, 39, 3, 52, 140, 47, 207},
				},
			},
		},
	},
	{
		broker:     NewBroker(2, "localhost", 4, 0, 0),
		desc:       "Chain length 2, position B2",
		thisNode:   "B2",
		chainRange: 2,
		ancestors: []string{
			"S0:B2",
			"S1:B2",
			"S2:B2",
			"S3:B2",
			"B2:B1",
			"B1:B0",
			"B0:P0,P1,P2,P3",
			"P0",
			"P1",
			"P2",
			"P3",
		},
		descendants: []string{
			"P0:B0",
			"P1:B0",
			"P2:B0",
			"B0:B1",
			"B1:B2",
			"B2:S0,S1,S2,S3",
			"S0",
			"S1",
			"S2",
			"S3",
		},
		pubReq: pb.Publication{
			PubType:       common.Chain,
			PublisherID:   1,
			PublicationID: 1,
			TopicID:       1,
			Contents: [][]byte{
				[]byte(common.Message1),
			},
			ChainMACs: []*pb.ChainMAC{
				&pb.ChainMAC{
					From: "B0",
					To:   "B2",
					MAC:  []byte{16, 237, 212, 97, 95, 176, 56, 90, 125, 103, 39, 3, 52, 140, 47, 207},
				},
				&pb.ChainMAC{
					From: "B1",
					To:   "S0",
					MAC:  []byte{16, 237, 212, 97, 95, 176, 56, 90, 125, 103, 39, 3, 52, 140, 47, 207},
				},
				&pb.ChainMAC{
					From: "B1",
					To:   "S1",
					MAC:  []byte{16, 237, 212, 97, 95, 176, 56, 90, 125, 103, 39, 3, 52, 140, 47, 207},
				},
				&pb.ChainMAC{
					From: "B1",
					To:   "S2",
					MAC:  []byte{16, 237, 212, 97, 95, 176, 56, 90, 125, 103, 39, 3, 52, 140, 47, 207},
				},
				&pb.ChainMAC{
					From: "B1",
					To:   "S3",
					MAC:  []byte{16, 237, 212, 97, 95, 176, 56, 90, 125, 103, 39, 3, 52, 140, 47, 207},
				},
			},
		},
		wantPubs: true,
		want: pb.Publication{
			PubType:       common.Chain,
			PublisherID:   1,
			PublicationID: 1,
			TopicID:       1,
			BrokerID:      2,
			Contents: [][]byte{
				[]byte(common.Message1),
			},
			ChainMACs: []*pb.ChainMAC{
				&pb.ChainMAC{
					From: "B1",
					To:   "S0",
					MAC:  []byte{16, 237, 212, 97, 95, 176, 56, 90, 125, 103, 39, 3, 52, 140, 47, 207},
				},
			},
		},
	},
	{
		broker:     NewBroker(1, "localhost", 4, 0, 0),
		desc:       "Chain length 3, position B1",
		thisNode:   "B1",
		chainRange: 3,
		ancestors: []string{
			"S0:B2",
			"S1:B2",
			"S2:B2",
			"S3:B2",
			"B2:B1",
			"B1:B0",
			"B0:P0,P1,P2,P3",
			"P0",
			"P1",
			"P2",
			"P3",
		},
		descendants: []string{
			"P0:B0",
			"P1:B0",
			"P2:B0",
			"B0:B1",
			"B1:B2",
			"B2:S0,S1,S2,S3",
			"S0",
			"S1",
			"S2",
			"S3",
		},
		pubReq: pb.Publication{
			PubType:       common.Chain,
			PublisherID:   1,
			PublicationID: 1,
			TopicID:       1,
			Contents: [][]byte{
				[]byte(common.Message1),
			},
			ChainMACs: []*pb.ChainMAC{
				&pb.ChainMAC{
					From: "P1",
					To:   "B1",
					MAC:  []byte{16, 237, 212, 97, 95, 176, 56, 90, 125, 103, 39, 3, 52, 140, 47, 207},
				},
				&pb.ChainMAC{
					From: "P1",
					To:   "B2",
					MAC:  []byte{16, 237, 212, 97, 95, 176, 56, 90, 125, 103, 39, 3, 52, 140, 47, 207},
				},
			},
		},
		wantPubs: true,
		want: pb.Publication{
			PubType:       common.Chain,
			PublisherID:   1,
			PublicationID: 1,
			TopicID:       1,
			BrokerID:      1,
			Contents: [][]byte{
				[]byte(common.Message1),
			},
			ChainMACs: []*pb.ChainMAC{
				&pb.ChainMAC{
					From: "P1",
					To:   "B2",
					MAC:  []byte{16, 237, 212, 97, 95, 176, 56, 90, 125, 103, 39, 3, 52, 140, 47, 207},
				},
				&pb.ChainMAC{
					From: "B1",
					To:   "S0",
					MAC:  []byte{16, 237, 212, 97, 95, 176, 56, 90, 125, 103, 39, 3, 52, 140, 47, 207},
				},
				&pb.ChainMAC{
					From: "B1",
					To:   "S2",
					MAC:  []byte{16, 237, 212, 97, 95, 176, 56, 90, 125, 103, 39, 3, 52, 140, 47, 207},
				},
				&pb.ChainMAC{
					From: "B1",
					To:   "S3",
					MAC:  []byte{16, 237, 212, 97, 95, 176, 56, 90, 125, 103, 39, 3, 52, 140, 47, 207},
				},
				&pb.ChainMAC{
					From: "B1",
					To:   "S4",
					MAC:  []byte{16, 237, 212, 97, 95, 176, 56, 90, 125, 103, 39, 3, 52, 140, 47, 207},
				},
			},
		},
	},
	{
		broker:     NewBroker(2, "localhost", 4, 0, 0),
		desc:       "Chain length 3, position B2",
		thisNode:   "B2",
		chainRange: 3,
		ancestors: []string{
			"S0:B2",
			"S1:B2",
			"S2:B2",
			"S3:B2",
			"B2:B1",
			"B1:B0",
			"B0:P0,P1,P2,P3",
			"P0",
			"P1",
			"P2",
			"P3",
		},
		descendants: []string{
			"P0:B0",
			"P1:B0",
			"P2:B0",
			"B0:B1",
			"B1:B2",
			"B2:S0,S1,S2,S3",
			"S0",
			"S1",
			"S2",
			"S3",
		},
		pubReq: pb.Publication{
			PubType:       common.Chain,
			PublisherID:   1,
			PublicationID: 1,
			TopicID:       1,
			Contents: [][]byte{
				[]byte(common.Message1),
			},
			ChainMACs: []*pb.ChainMAC{
				&pb.ChainMAC{
					From: "P0",
					To:   "B2",
					MAC:  []byte{16, 237, 212, 97, 95, 176, 56, 90, 125, 103, 39, 3, 52, 140, 47, 207},
				},
				&pb.ChainMAC{
					From: "B0",
					To:   "B2",
					MAC:  []byte{16, 237, 212, 97, 95, 176, 56, 90, 125, 103, 39, 3, 52, 140, 47, 207},
				},
				&pb.ChainMAC{
					From: "B0",
					To:   "S0",
					MAC:  []byte{16, 237, 212, 97, 95, 176, 56, 90, 125, 103, 39, 3, 52, 140, 47, 207},
				},
				&pb.ChainMAC{
					From: "B0",
					To:   "S1",
					MAC:  []byte{16, 237, 212, 97, 95, 176, 56, 90, 125, 103, 39, 3, 52, 140, 47, 207},
				},
				&pb.ChainMAC{
					From: "B0",
					To:   "S2",
					MAC:  []byte{16, 237, 212, 97, 95, 176, 56, 90, 125, 103, 39, 3, 52, 140, 47, 207},
				},
				&pb.ChainMAC{
					From: "B0",
					To:   "S3",
					MAC:  []byte{16, 237, 212, 97, 95, 176, 56, 90, 125, 103, 39, 3, 52, 140, 47, 207},
				},
				&pb.ChainMAC{
					From: "B1",
					To:   "S0",
					MAC:  []byte{16, 237, 212, 97, 95, 176, 56, 90, 125, 103, 39, 3, 52, 140, 47, 207},
				},
				&pb.ChainMAC{
					From: "B1",
					To:   "S1",
					MAC:  []byte{16, 237, 212, 97, 95, 176, 56, 90, 125, 103, 39, 3, 52, 140, 47, 207},
				},
				&pb.ChainMAC{
					From: "B1",
					To:   "S2",
					MAC:  []byte{16, 237, 212, 97, 95, 176, 56, 90, 125, 103, 39, 3, 52, 140, 47, 207},
				},
				&pb.ChainMAC{
					From: "B1",
					To:   "S3",
					MAC:  []byte{16, 237, 212, 97, 95, 176, 56, 90, 125, 103, 39, 3, 52, 140, 47, 207},
				},
			},
		},
		wantPubs: true,
		want: pb.Publication{
			PubType:       common.Chain,
			PublisherID:   1,
			PublicationID: 1,
			TopicID:       1,
			BrokerID:      2,
			Contents: [][]byte{
				[]byte(common.Message1),
			},
			ChainMACs: []*pb.ChainMAC{
				&pb.ChainMAC{
					From: "B0",
					To:   "S0",
					MAC:  []byte{16, 237, 212, 97, 95, 176, 56, 90, 125, 103, 39, 3, 52, 140, 47, 207},
				},
				&pb.ChainMAC{
					From: "B1",
					To:   "S1",
					MAC:  []byte{16, 237, 212, 97, 95, 176, 56, 90, 125, 103, 39, 3, 52, 140, 47, 207},
				},
			},
		},
	},
}
