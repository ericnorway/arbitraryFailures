package main

import (
	"testing"

	"github.com/ericnorway/arbitraryFailures/common"
	pb "github.com/ericnorway/arbitraryFailures/proto"
)

func TestHandleBrbPublish(t *testing.T) {
	for i, test := range handleBrbPublishs {

		// Manually add broker channels
		for j := 0; j < test.numSubscribers; j++ {
			test.broker.toBrokerEchoChs.AddToBrokerEchoChannel(int64(j))
		}

		for j, subtest := range test.subtests {
			// Add publication request
			test.broker.handleBrbPublish(&subtest.pubReq)

			// Check that all "brokers" got the echoed publication
			test.broker.toSubscriberChs.RLock()
			for _, ch := range test.broker.toSubscriberChs.chs {
				select {
				case pub := <-ch:
					if !Equals(*pub, subtest.want) {
						t.Errorf("HandleBrbPublish\ntest nr:%d\ndescription: %s\naction nr: %d\nwant: %v\ngot: %v\n",
							i+1, test.desc, j+1, &subtest.want, pub)
					}
				}
			}
			test.broker.toSubscriberChs.RUnlock()
		}
	}
}

type handleBrbPublish struct {
	pubReq pb.Publication
	want   pb.Publication
}

var handleBrbPublishs = []struct {
	broker         *Broker
	desc           string
	numSubscribers int
	subtests       []handleBrbPublish
}{
	{
		broker:         NewBroker(),
		desc:           "1 pub request, 1 subscriber",
		numSubscribers: 1,
		subtests: []handleBrbPublish{
			{
				pubReq: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					Topic:         1,
					Content:       []byte(message1),
				},
				want: pb.Publication{
					PubType:       common.BRB,
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
		broker:         NewBroker(),
		desc:           "1 pub request, 3 subscribers",
		numSubscribers: 3,
		subtests: []handleBrbPublish{
			{
				pubReq: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					Topic:         2,
					Content:       []byte(message1),
				},
				want: pb.Publication{
					PubType:       common.BRB,
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
		broker:         NewBroker(),
		desc:           "5 pub requests, 3 subscribers",
		numSubscribers: 3,
		subtests: []handleBrbPublish{
			{
				pubReq: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					Topic:         1,
					Content:       []byte(message1),
				},
				want: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					Topic:         1,
					BrokerID:      0,
					Content:       []byte(message1),
				},
			},
			{
				pubReq: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 2,
					Topic:         1,
					Content:       []byte(message2),
				},
				want: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 2,
					Topic:         1,
					BrokerID:      0,
					Content:       []byte(message2),
				},
			},
			{
				pubReq: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   2,
					PublicationID: 1,
					Topic:         2,
					Content:       []byte(message3),
				},
				want: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   2,
					PublicationID: 1,
					Topic:         2,
					BrokerID:      0,
					Content:       []byte(message3),
				},
			},
			{
				pubReq: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   2,
					PublicationID: 2,
					Topic:         3,
					Content:       []byte(message4),
				},
				want: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   2,
					PublicationID: 2,
					Topic:         3,
					BrokerID:      0,
					Content:       []byte(message4),
				},
			},
			{
				pubReq: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   3,
					PublicationID: 1,
					Topic:         1,
					Content:       []byte(message1),
				},
				want: pb.Publication{
					PubType:       common.BRB,
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
