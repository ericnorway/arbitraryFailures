package broker

import (
	//"fmt"
	"testing"

	"github.com/ericnorway/arbitraryFailures/common"
	pb "github.com/ericnorway/arbitraryFailures/proto"
)

func TestHandleAbPublish(t *testing.T) {
	for i, test := range handleAbPublishTests {

		// Manually add subscriber channels and topics
		for j := 0; j < test.numSubscribers; j++ {
			test.broker.AddSubscriber(uint64(j), []byte("12345"))
			test.broker.addToSubChannel(uint64(j))
			test.broker.subscribers[uint64(j)].topics[1] = true
			test.broker.subscribers[uint64(j)].topics[2] = true
			test.broker.subscribers[uint64(j)].topics[3] = true
		}

		for j, subtest := range test.subtests {
			// Add publication request
			test.broker.handleAbPublish(&subtest.pubReq)

			// Check that all "subscribers" got the forwarded publication
			test.broker.subscribersMutex.RLock()
			for _, subscriber := range test.broker.subscribers {
				select {
				case pub := <-subscriber.toCh:
					if !common.Equals(pub, subtest.want) {
						t.Errorf("HandleAbPublish\ntest nr:%d\ndescription: %s\naction nr: %d\nwant: %v\ngot: %v\n",
							i+1, test.desc, j+1, &subtest.want, pub)
					}
				}
			}
			test.broker.subscribersMutex.RUnlock()
		}
	}
}

type handleAbPublishTest struct {
	pubReq pb.Publication
	want   pb.Publication
}

var handleAbPublishTests = []struct {
	broker         *Broker
	desc           string
	numSubscribers int
	subtests       []handleAbPublishTest
}{
	{
		broker:         NewBroker(0, "localhost", 0, 0),
		desc:           "1 pub request, 1 subscriber",
		numSubscribers: 1,
		subtests: []handleAbPublishTest{
			{
				pubReq: pb.Publication{
					PubType:       common.AB,
					PublisherID:   1,
					PublicationID: 1,
					TopicID:       1,
					Contents: [][]byte{
						[]byte(common.Message1),
					},
				},
				want: pb.Publication{
					PubType:       common.AB,
					PublisherID:   1,
					PublicationID: 1,
					TopicID:       1,
					BrokerID:      0,
					Contents: [][]byte{
						[]byte(common.Message1),
					},
				},
			},
		},
	},
	{
		broker:         NewBroker(0, "localhost", 0, 0),
		desc:           "1 pub request, 3 subscribers",
		numSubscribers: 3,
		subtests: []handleAbPublishTest{
			{
				pubReq: pb.Publication{
					PubType:       common.AB,
					PublisherID:   1,
					PublicationID: 1,
					TopicID:       2,
					Contents: [][]byte{
						[]byte(common.Message1),
					},
				},
				want: pb.Publication{
					PubType:       common.AB,
					PublisherID:   1,
					PublicationID: 1,
					TopicID:       2,
					BrokerID:      0,
					Contents: [][]byte{
						[]byte(common.Message1),
					},
				},
			},
		},
	},
	{
		broker:         NewBroker(0, "localhost", 0, 0),
		desc:           "5 pub requests, 3 subscribers",
		numSubscribers: 3,
		subtests: []handleAbPublishTest{
			{
				pubReq: pb.Publication{
					PubType:       common.AB,
					PublisherID:   1,
					PublicationID: 1,
					TopicID:       1,
					Contents: [][]byte{
						[]byte(common.Message1),
					},
				},
				want: pb.Publication{
					PubType:       common.AB,
					PublisherID:   1,
					PublicationID: 1,
					TopicID:       1,
					BrokerID:      0,
					Contents: [][]byte{
						[]byte(common.Message1),
					},
				},
			},
			{
				pubReq: pb.Publication{
					PubType:       common.AB,
					PublisherID:   1,
					PublicationID: 2,
					TopicID:       1,
					Contents: [][]byte{
						[]byte(common.Message2),
					},
				},
				want: pb.Publication{
					PubType:       common.AB,
					PublisherID:   1,
					PublicationID: 2,
					TopicID:       1,
					BrokerID:      0,
					Contents: [][]byte{
						[]byte(common.Message2),
					},
				},
			},
			{
				pubReq: pb.Publication{
					PubType:       common.AB,
					PublisherID:   2,
					PublicationID: 1,
					TopicID:       2,
					Contents: [][]byte{
						[]byte(common.Message3),
					},
				},
				want: pb.Publication{
					PubType:       common.AB,
					PublisherID:   2,
					PublicationID: 1,
					TopicID:       2,
					BrokerID:      0,
					Contents: [][]byte{
						[]byte(common.Message3),
					},
				},
			},
			{
				pubReq: pb.Publication{
					PubType:       common.AB,
					PublisherID:   2,
					PublicationID: 2,
					TopicID:       3,
					Contents: [][]byte{
						[]byte(common.Message4),
					},
				},
				want: pb.Publication{
					PubType:       common.AB,
					PublisherID:   2,
					PublicationID: 2,
					TopicID:       3,
					BrokerID:      0,
					Contents: [][]byte{
						[]byte(common.Message4),
					},
				},
			},
			{
				pubReq: pb.Publication{
					PubType:       common.AB,
					PublisherID:   3,
					PublicationID: 1,
					TopicID:       1,
					Contents: [][]byte{
						[]byte(common.Message1),
					},
				},
				want: pb.Publication{
					PubType:       common.AB,
					PublisherID:   3,
					PublicationID: 1,
					TopicID:       1,
					BrokerID:      0,
					Contents: [][]byte{
						[]byte(common.Message1),
					},
				},
			},
		},
	},
}
