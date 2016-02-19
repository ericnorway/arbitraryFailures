package main

import (
	"testing"

	"github.com/ericnorway/arbitraryFailures/common"
	pb "github.com/ericnorway/arbitraryFailures/proto"
)

func TestHandleBrbPublish(t *testing.T) {
	for i, test := range handleBrbPublishTests {

		// Manually add other broker channels
		for j := 1; j < test.numBrokers; j++ {
			test.broker.toBrokerEchoChs.AddToBrokerEchoChannel(int64(j))
		}

		for j, subtest := range test.subtests {
			// Add publication request
			test.broker.handleBrbPublish(&subtest.pubReq)

			// Check that all other "brokers" got the echoed publication
			test.broker.toBrokerEchoChs.RLock()
			for _, ch := range test.broker.toBrokerEchoChs.chs {
				select {
				case pub := <-ch:
					if !Equals(*pub, subtest.want) {
						t.Errorf("HandleBrbPublish\ntest nr:%d\ndescription: %s\naction nr: %d\nwant: %v\ngot: %v\n",
							i+1, test.desc, j+1, &subtest.want, pub)
					}
				}
			}
			test.broker.toBrokerEchoChs.RUnlock()
		}
	}
}

type handleBrbPublishTest struct {
	pubReq pb.Publication
	want   pb.Publication
}

var handleBrbPublishTests = []struct {
	broker         *Broker
	desc           string
	numBrokers     int
	subtests       []handleBrbPublishTest
}{
	{
		broker:         NewBroker(),
		desc:           "1 pub request, 3 other brokers",
		numBrokers: 4,
		subtests: []handleBrbPublishTest{
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
		desc:           "5 pub requests, 3 other brokers",
		numBrokers: 3,
		subtests: []handleBrbPublishTest{
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

func TestHandleBrbEcho(t *testing.T) {
	for i, test := range handleBrbEchoTests {

		// Manually add other broker channels
		for j := 1; j < test.numBrokers; j++ {
			test.broker.toBrokerReadyChs.AddToBrokerReadyChannel(int64(j))
		}
		
		// Manually add subscriber channels
		for j := 0; j < test.numSubscribers; j++ {
			test.broker.toSubscriberChs.AddToSubscriberChannel(int64(j))
			test.broker.topics[int64(j)] = make(map[int64]bool)
			test.broker.topics[int64(j)][1] = true
			test.broker.topics[int64(j)][2] = true
			test.broker.topics[int64(j)][3] = true
		}

		for j, subtest := range test.subtests {
			// Add echo requests
			for _, echo := range subtest.echoes {
				test.broker.handleEcho(&echo)
			}

			// Check that all other "brokers" got the readied publication
			test.broker.toBrokerReadyChs.RLock()
			for _, ch := range test.broker.toBrokerReadyChs.chs {
				select {
				case pub := <-ch:
					if !Equals(*pub, subtest.want) {
						t.Errorf("B HandleBrbPublish\ntest nr:%d\ndescription: %s\naction nr: %d\nwant: %v\ngot: %v\n",
							i+1, test.desc, j+1, &subtest.want, pub)
					}
				}
			}
			test.broker.toBrokerReadyChs.RUnlock()
			
			// Check that all "subscribers" got the readied publication
			test.broker.toSubscriberChs.RLock()
			for _, ch := range test.broker.toSubscriberChs.chs {
				select {
				case pub := <-ch:
					if !Equals(*pub, subtest.want) {
						t.Errorf("S HandleBrbPublish\ntest nr:%d\ndescription: %s\naction nr: %d\nwant: %v\ngot: %v\n",
							i+1, test.desc, j+1, &subtest.want, pub)
					}
				}
			}
			test.broker.toSubscriberChs.RUnlock()
		}
	}
}

type handleBrbEchoTest struct {
	echoes []pb.Publication
	want   pb.Publication
}

var handleBrbEchoTests = []struct {
	broker         *Broker
	desc           string
	numBrokers     int
	numSubscribers int
	subtests       []handleBrbEchoTest
}{
	{
		broker:         NewBroker(),
		desc:           "1 pub request, 3 other brokers, 1 subscriber",
		numBrokers: 4,
		numSubscribers: 1,
		subtests: []handleBrbEchoTest{
			{
				echoes: []pb.Publication{
					{
							PubType:       common.BRB,
							PublisherID:   1,
							PublicationID: 1,
							BrokerID:      0,
							Topic:         1,
							Content:       []byte(message1),
					},
					{
							PubType:       common.BRB,
							PublisherID:   1,
							PublicationID: 1,
							BrokerID:      1,
							Topic:         1,
							Content:       []byte(message1),
					},
					{
							PubType:       common.BRB,
							PublisherID:   1,
							PublicationID: 1,
							BrokerID:      2,
							Topic:         1,
							Content:       []byte(message1),
					},
					{
							PubType:       common.BRB,
							PublisherID:   1,
							PublicationID: 1,
							BrokerID:      3,
							Topic:         1,
							Content:       []byte(message1),
					},
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
}

