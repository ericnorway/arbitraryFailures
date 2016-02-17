package main

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/ericnorway/arbitraryFailures/common"
	pb "github.com/ericnorway/arbitraryFailures/proto"
)

func TestAbProcessing(t *testing.T) {
	for i, test := range abProcessingTests {
		go test.broker.handleMessages()

		// Add "subscribers" (channels)
		for j := 0; j < test.numSubscribers; j++ {
			test.broker.subscribers.AddSubscriberInfo(int64(j), strconv.Itoa(j), []int64{1, 2, 3})
		}

		for j, subtest := range test.subtests {
			// Add publication request
			test.broker.publishers.fromPublisherCh<- &subtest.pubReq

			// Check that all "subscribers" got the forwarded publication
			test.broker.subscribers.RLock()
			for _, subscriber := range test.broker.subscribers.subscribers {
				select {
				case pub := <-subscriber.toSubscriberCh:
					if !Equals(*pub, subtest.want) {
						t.Errorf("AbProcessing\ntest nr:%d\ndescription: %s\naction nr: %d\nwant: %v\ngot: %v\n",
							i+1, test.desc, j+1, &subtest.want, pub)
					}
				}
			}
			test.broker.subscribers.RUnlock()
		}
	}
}

type abProcessingTest struct {
	pubReq pb.Publication
	want   pb.Publication
}

var message1 = "Some data."
var message2 = "Some other data."
var message3 = "Some more data."
var message4 = "Even more data..."

func Equals(a pb.Publication, b pb.Publication) bool {
	if a.PubType != b.PubType {
		return false
	}
	if a.PublisherID != b.PublisherID {
		return false
	}
	if a.PublicationID != b.PublicationID {
		return false
	}
	if a.Topic != b.Topic {
		return false
	}
	if a.BrokerID != b.BrokerID {
		return false
	}
	if !bytes.Equal(a.Content, b.Content) {
		return false
	}
	return true
}

var abProcessingTests = []struct {
	broker         *Broker
	desc           string
	numSubscribers int
	subtests       []abProcessingTest
}{
	{
		broker:         NewBroker(),
		desc:           "1 pub request, 1 subscriber",
		numSubscribers: 1,
		subtests: []abProcessingTest{
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
		broker:         NewBroker(),
		desc:           "1 pub request, 3 subscribers",
		numSubscribers: 3,
		subtests: []abProcessingTest{
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
		broker:         NewBroker(),
		desc:           "5 pub requests, 3 subscribers",
		numSubscribers: 3,
		subtests: []abProcessingTest{
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
