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
			test.broker.AddSubscriber(int64(j), []byte("12345"))
			test.broker.addToSubChannel(int64(j))
			test.broker.subscribers[int64(j)].topics[1] = true
			test.broker.subscribers[int64(j)].topics[2] = true
			test.broker.subscribers[int64(j)].topics[3] = true
		}

		for j, subtest := range test.subtests {
			// Add publication request
			test.broker.handleAbPublish(&subtest.pubReq)

			// Check that all "subscribers" got the forwarded publication
			test.broker.subscribersMutex.RLock()
			for _, subscriber := range test.broker.subscribers {
				select {
				case pub := <-subscriber.toCh:
					if !Equals(pub, subtest.want) {
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
		broker:         NewBroker(0, "localhost", 0),
		desc:           "1 pub request, 1 subscriber",
		numSubscribers: 1,
		subtests: []handleAbPublishTest{
			{
				pubReq: pb.Publication{
					PubType:       common.AB,
					PublisherID:   1,
					PublicationID: 1,
					Topic:         1,
					Content:       []byte(message1),
				},
				want: pb.Publication{
					PubType:       common.AB,
					PublisherID:   1,
					PublicationID: 1,
					Topic:         1,
					BrokerID:      0,
					Content:       []byte(message1),
				},
			},
		},
	},
	{
		broker:         NewBroker(0, "localhost", 0),
		desc:           "1 pub request, 3 subscribers",
		numSubscribers: 3,
		subtests: []handleAbPublishTest{
			{
				pubReq: pb.Publication{
					PubType:       common.AB,
					PublisherID:   1,
					PublicationID: 1,
					Topic:         2,
					Content:       []byte(message1),
				},
				want: pb.Publication{
					PubType:       common.AB,
					PublisherID:   1,
					PublicationID: 1,
					Topic:         2,
					BrokerID:      0,
					Content:       []byte(message1),
				},
			},
		},
	},
	{
		broker:         NewBroker(0, "localhost", 0),
		desc:           "5 pub requests, 3 subscribers",
		numSubscribers: 3,
		subtests: []handleAbPublishTest{
			{
				pubReq: pb.Publication{
					PubType:       common.AB,
					PublisherID:   1,
					PublicationID: 1,
					Topic:         1,
					Content:       []byte(message1),
				},
				want: pb.Publication{
					PubType:       common.AB,
					PublisherID:   1,
					PublicationID: 1,
					Topic:         1,
					BrokerID:      0,
					Content:       []byte(message1),
				},
			},
			{
				pubReq: pb.Publication{
					PubType:       common.AB,
					PublisherID:   1,
					PublicationID: 2,
					Topic:         1,
					Content:       []byte(message2),
				},
				want: pb.Publication{
					PubType:       common.AB,
					PublisherID:   1,
					PublicationID: 2,
					Topic:         1,
					BrokerID:      0,
					Content:       []byte(message2),
				},
			},
			{
				pubReq: pb.Publication{
					PubType:       common.AB,
					PublisherID:   2,
					PublicationID: 1,
					Topic:         2,
					Content:       []byte(message3),
				},
				want: pb.Publication{
					PubType:       common.AB,
					PublisherID:   2,
					PublicationID: 1,
					Topic:         2,
					BrokerID:      0,
					Content:       []byte(message3),
				},
			},
			{
				pubReq: pb.Publication{
					PubType:       common.AB,
					PublisherID:   2,
					PublicationID: 2,
					Topic:         3,
					Content:       []byte(message4),
				},
				want: pb.Publication{
					PubType:       common.AB,
					PublisherID:   2,
					PublicationID: 2,
					Topic:         3,
					BrokerID:      0,
					Content:       []byte(message4),
				},
			},
			{
				pubReq: pb.Publication{
					PubType:       common.AB,
					PublisherID:   3,
					PublicationID: 1,
					Topic:         1,
					Content:       []byte(message1),
				},
				want: pb.Publication{
					PubType:       common.AB,
					PublisherID:   3,
					PublicationID: 1,
					Topic:         1,
					BrokerID:      0,
					Content:       []byte(message1),
				},
			},
		},
	},
}
