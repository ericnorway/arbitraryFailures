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
	broker     *Broker
	desc       string
	numBrokers int
	subtests   []handleBrbPublishTest
}{
	{
		broker:     NewBroker(),
		desc:       "1 pub request, 3 other brokers",
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
		broker:     NewBroker(),
		desc:       "5 pub requests, 3 other brokers",
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
			// Add echo request
			test.broker.handleEcho(&subtest.echo)

			if subtest.output == true {
				// Check that all other "brokers" got the readied publication
				test.broker.toBrokerReadyChs.RLock()
				for _, ch := range test.broker.toBrokerReadyChs.chs {
					if len(ch) != 1 {
						t.Errorf("HandleBrbEcho\ntest nr:%d\ndescription: %s\naction nr: %d\nBroker channel should have 1.\nThere are %v publications.\n",
							i+1, test.desc, j+1, len(ch))
						continue
					}
					select {
					case pub := <-ch:
						if !Equals(*pub, subtest.want) {
							t.Errorf("HandleBrbEcho\ntest nr:%d\ndescription: %s\naction nr: %d\nwant: %v\ngot: %v\n",
								i+1, test.desc, j+1, &subtest.want, pub)
						}
					}
				}
				test.broker.toBrokerReadyChs.RUnlock()

				// Check that all "subscribers" got the readied publication
				test.broker.toSubscriberChs.RLock()
				for _, ch := range test.broker.toSubscriberChs.chs {
					if len(ch) != 1 {
						t.Errorf("HandleBrbEcho\ntest nr:%d\ndescription: %s\naction nr: %d\nSub channel should have 1.\nThere are %v publications.\n",
							i+1, test.desc, j+1, len(ch))
						continue
					}
					select {
					case pub := <-ch:
						if !Equals(*pub, subtest.want) {
							t.Errorf("HandleBrbEcho\ntest nr:%d\ndescription: %s\naction nr: %d\nwant: %v\ngot: %v\n",
								i+1, test.desc, j+1, &subtest.want, pub)
						}
					}
				}
				test.broker.toSubscriberChs.RUnlock()
			} else {
				// Check that all other "brokers" have empty channels
				test.broker.toBrokerReadyChs.RLock()
				for _, ch := range test.broker.toBrokerReadyChs.chs {
					if len(ch) > 0 {
						t.Errorf("HandleBrbEcho\ntest nr:%d\ndescription: %s\naction nr: %d\nBroker channel should be empty.\nThere is %v publication(s).\n",
							i+1, test.desc, j+1, len(ch))
					}
				}
				test.broker.toBrokerReadyChs.RUnlock()

				// Check that all "subscribers" have empty channels
				test.broker.toSubscriberChs.RLock()
				for _, ch := range test.broker.toSubscriberChs.chs {
					if len(ch) > 0 {
						t.Errorf("HandleBrbEcho\ntest nr:%d\ndescription: %s\naction nr: %d\nSub channel should be empty.\nThere is %v publication(s).\n",
							i+1, test.desc, j+1, len(ch))
					}
				}
				test.broker.toSubscriberChs.RUnlock()
			}
		}
	}
}

type handleBrbEchoTest struct {
	echo   pb.Publication
	output bool
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
		desc:           "4 x 1 echoes (4 echoes for 1 publication), 3 other brokers, 2 subscriber",
		numBrokers:     4,
		numSubscribers: 2,
		subtests: []handleBrbEchoTest{
			{
				echo: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      0,
					Topic:         1,
					Content:       []byte(message1),
				},
				output: false,
				want:   pb.Publication{},
			},
			{
				echo: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      1,
					Topic:         1,
					Content:       []byte(message1),
				},
				output: false,
				want:   pb.Publication{},
			},
			{
				echo: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      2,
					Topic:         1,
					Content:       []byte(message1),
				},
				output: true,
				want: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      0,
					Topic:         1,
					Content:       []byte(message1),
				},
			},
			{
				echo: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      3,
					Topic:         1,
					Content:       []byte(message1),
				},
				output: false,
				want:   pb.Publication{},
			},
		},
	},
	{
		broker:         NewBroker(),
		desc:           "4 x 1, 2 x 1, 3 x 1 echoes, 3 other brokers, 1 subscriber",
		numBrokers:     4,
		numSubscribers: 1,
		subtests: []handleBrbEchoTest{
			{
				echo: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      0,
					Topic:         1,
					Content:       []byte(message1),
				},
				output: false,
				want:   pb.Publication{},
			},
			{
				echo: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   2,
					PublicationID: 3,
					BrokerID:      1,
					Topic:         2,
					Content:       []byte(message3),
				},
				output: false,
				want:   pb.Publication{},
			},
			{
				echo: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      1,
					Topic:         1,
					Content:       []byte(message1),
				},
				output: false,
				want:   pb.Publication{},
			},
			{
				echo: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 2,
					BrokerID:      0,
					Topic:         3,
					Content:       []byte(message2),
				},
				output: false,
				want:   pb.Publication{},
			},
			{
				echo: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      2,
					Topic:         1,
					Content:       []byte(message1),
				},
				output: true,
				want: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      0,
					Topic:         1,
					Content:       []byte(message1),
				},
			},
			{
				echo: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      3,
					Topic:         1,
					Content:       []byte(message1),
				},
				output: false,
				want:   pb.Publication{},
			},
			{
				echo: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 2,
					BrokerID:      1,
					Topic:         3,
					Content:       []byte(message2),
				},
				output: false,
				want:   pb.Publication{},
			},
			{
				echo: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   2,
					PublicationID: 3,
					BrokerID:      0,
					Topic:         2,
					Content:       []byte(message3),
				},
				output: false,
				want:   pb.Publication{},
			},
			{
				echo: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   2,
					PublicationID: 3,
					BrokerID:      2,
					Topic:         2,
					Content:       []byte(message3),
				},
				output: true,
				want: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   2,
					PublicationID: 3,
					BrokerID:      0,
					Topic:         2,
					Content:       []byte(message3),
				},
			},
		},
	},
	{
		broker:         NewBroker(),
		desc:           "4 x 1 echoes (1 with wrong content), 3 other brokers, 2 subscriber",
		numBrokers:     4,
		numSubscribers: 2,
		subtests: []handleBrbEchoTest{
			{
				echo: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      0,
					Topic:         1,
					Content:       []byte(message2),
				},
				output: false,
				want:   pb.Publication{},
			},
			{
				echo: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      1,
					Topic:         1,
					Content:       []byte(message4),
				},
				output: false,
				want:   pb.Publication{},
			},
			{
				echo: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      2,
					Topic:         1,
					Content:       []byte(message2),
				},
				output: false,
				want:   pb.Publication{},
			},
			{
				echo: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      3,
					Topic:         1,
					Content:       []byte(message2),
				},
				output: true,
				want: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      0,
					Topic:         1,
					Content:       []byte(message2),
				},
			},
		},
	},
	{
		broker:         NewBroker(),
		desc:           "4 x 1 echoes (4 echoes for 1 publication, 1 with wrong topic), 3 other brokers, 2 subscriber",
		numBrokers:     4,
		numSubscribers: 2,
		subtests: []handleBrbEchoTest{
			{
				echo: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      0,
					Topic:         1,
					Content:       []byte(message1),
				},
				output: false,
				want:   pb.Publication{},
			},
			{
				echo: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      1,
					Topic:         7,
					Content:       []byte(message1),
				},
				output: false,
				want:   pb.Publication{},
			},
			{
				echo: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      2,
					Topic:         1,
					Content:       []byte(message1),
				},
				output: false,
				want:   pb.Publication{},
			},
			{
				echo: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      3,
					Topic:         1,
					Content:       []byte(message1),
				},
				output: true,
				want: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      0,
					Topic:         1,
					Content:       []byte(message1),
				},
			},
		},
	},
}

func TestHandleBrbReady(t *testing.T) {
	for i, test := range handleBrbReadyTests {

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

		// Manually add the publications already readied
		for _, pub := range test.alreadyReadied {
			if test.broker.readiesSent[pub.PublisherID] == nil {
				test.broker.readiesSent[pub.PublisherID] = make(map[int64]bool)
			}
			test.broker.readiesSent[pub.PublisherID][pub.PublicationID] = true
		}

		for j, subtest := range test.subtests {
			// Add ready request
			test.broker.handleReady(&subtest.ready)

			if subtest.output == true {
				// Check that all other "brokers" got the readied publication
				test.broker.toBrokerReadyChs.RLock()
				for _, ch := range test.broker.toBrokerReadyChs.chs {
					if len(ch) != 1 {
						t.Errorf("HandleBrbReady\ntest nr:%d\ndescription: %s\naction nr: %d\nBroker channel should have 1.\nThere are %v publications.\n",
							i+1, test.desc, j+1, len(ch))
						continue
					}
					select {
					case pub := <-ch:
						if !Equals(*pub, subtest.want) {
							t.Errorf("HandleBrbReady\ntest nr:%d\ndescription: %s\naction nr: %d\nwant: %v\ngot: %v\n",
								i+1, test.desc, j+1, &subtest.want, pub)
						}
					}
				}
				test.broker.toBrokerReadyChs.RUnlock()

				// Check that all "subscribers" got the readied publication
				test.broker.toSubscriberChs.RLock()
				for _, ch := range test.broker.toSubscriberChs.chs {
					if len(ch) != 1 {
						t.Errorf("HandleBrbReady\ntest nr:%d\ndescription: %s\naction nr: %d\nSub channel should have 1.\nThere are %v publications.\n",
							i+1, test.desc, j+1, len(ch))
						continue
					}
					select {
					case pub := <-ch:
						if !Equals(*pub, subtest.want) {
							t.Errorf("HandleBrbReady\ntest nr:%d\ndescription: %s\naction nr: %d\nwant: %v\ngot: %v\n",
								i+1, test.desc, j+1, &subtest.want, pub)
						}
					}
				}
				test.broker.toSubscriberChs.RUnlock()
			} else {
				// Check that all other "brokers" have empty channels
				test.broker.toBrokerReadyChs.RLock()
				for _, ch := range test.broker.toBrokerReadyChs.chs {
					if len(ch) > 0 {
						t.Errorf("HandleBrbReady\ntest nr:%d\ndescription: %s\naction nr: %d\nBroker channel should be empty.\nThere is %v publication(s).\n",
							i+1, test.desc, j+1, len(ch))
					}
				}
				test.broker.toBrokerReadyChs.RUnlock()

				// Check that all "subscribers" have empty channels
				test.broker.toSubscriberChs.RLock()
				for _, ch := range test.broker.toSubscriberChs.chs {
					if len(ch) > 0 {
						t.Errorf("HandleBrbReady\ntest nr:%d\ndescription: %s\naction nr: %d\nSub channel should be empty.\nThere is %v publication(s).\n",
							i+1, test.desc, j+1, len(ch))
					}
				}
				test.broker.toSubscriberChs.RUnlock()
			}
		}
	}
}

type handleBrbReadyTest struct {
	ready  pb.Publication
	output bool
	want   pb.Publication
}

var handleBrbReadyTests = []struct {
	broker         *Broker
	desc           string
	numBrokers     int
	numSubscribers int
	alreadyReadied []pb.Publication
	subtests       []handleBrbReadyTest
}{
	{
		broker:         NewBroker(),
		desc:           "4 x 1 readies (4 readies for 1 publication (not readied yet)), 3 other brokers, 2 subscriber",
		numBrokers:     4,
		numSubscribers: 2,
		alreadyReadied: []pb.Publication{},
		subtests: []handleBrbReadyTest{
			{
				ready: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      0,
					Topic:         1,
					Content:       []byte(message1),
				},
				output: false,
				want:   pb.Publication{},
			},
			{
				ready: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      1,
					Topic:         1,
					Content:       []byte(message1),
				},
				output: true,
				want: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      0,
					Topic:         1,
					Content:       []byte(message1),
				},
			},
			{
				ready: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      2,
					Topic:         1,
					Content:       []byte(message1),
				},
				output: false,
				want:   pb.Publication{},
			},
			{
				ready: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      3,
					Topic:         1,
					Content:       []byte(message1),
				},
				output: false,
				want:   pb.Publication{},
			},
		},
	},
	{
		broker:         NewBroker(),
		desc:           "4 x 1 readies (4 readies for 1 publication (already readied)), 3 other brokers, 2 subscriber",
		numBrokers:     4,
		numSubscribers: 2,
		alreadyReadied: []pb.Publication{
			{
				PubType:       common.BRB,
				PublisherID:   1,
				PublicationID: 1,
				BrokerID:      0,
				Topic:         1,
				Content:       []byte(message1),
			},
		},
		subtests: []handleBrbReadyTest{
			{
				ready: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      0,
					Topic:         1,
					Content:       []byte(message1),
				},
				output: false,
				want:   pb.Publication{},
			},
			{
				ready: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      1,
					Topic:         1,
					Content:       []byte(message1),
				},
				output: false,
				want:   pb.Publication{},
			},
			{
				ready: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      2,
					Topic:         1,
					Content:       []byte(message1),
				},
				output: false,
				want:   pb.Publication{},
			},
			{
				ready: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      3,
					Topic:         1,
					Content:       []byte(message1),
				},
				output: false,
				want:   pb.Publication{},
			},
		},
	},
	{
		broker:         NewBroker(),
		desc:           "4 x 1 readies (4 readies for 1 publication (not readied yet, 1 ready has a different topic)), 3 other brokers, 2 subscriber",
		numBrokers:     4,
		numSubscribers: 2,
		alreadyReadied: []pb.Publication{},
		subtests: []handleBrbReadyTest{
			{
				ready: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      0,
					Topic:         1,
					Content:       []byte(message1),
				},
				output: false,
				want:   pb.Publication{},
			},
			{
				ready: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      1,
					Topic:         2,
					Content:       []byte(message1),
				},
				output: false,
				want:   pb.Publication{},
			},
			{
				ready: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      2,
					Topic:         1,
					Content:       []byte(message1),
				},
				output: true,
				want: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      0,
					Topic:         1,
					Content:       []byte(message1),
				},
			},
			{
				ready: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 1,
					BrokerID:      3,
					Topic:         1,
					Content:       []byte(message1),
				},
				output: false,
				want:   pb.Publication{},
			},
		},
	},
}
