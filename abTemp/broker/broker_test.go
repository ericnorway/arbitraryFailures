package main

import (
	"bytes"
	"strconv"
	"testing"
	
	pb "github.com/ericnorway/arbitraryFailures/abTemp/proto"
)

func TestAbProcessing(t *testing.T) {
	for i, test := range abProcessingTests {
		go test.broker.processAbMessages()

		// Add "subscribers" (channels)
		for j := 0; j < test.numSubscribers; j++ {
			test.broker.abAddChannel(strconv.Itoa(j))
		}
		
		for j, subtest := range test.subtests {
			// Add publication request
			test.broker.abPubChan<- &subtest.pubReq
			
			// Check that all "subscribers" got the forwarded publication
			test.broker.abFwdChansMutex.RLock()
			for _, ch := range test.broker.abFwdChans {
				select {
					case fwdPub := <-ch:
						if !Equals(*fwdPub, subtest.want) {
							t.Errorf("AbProcessing\ntest nr:%d\ndescription: %s\naction nr: %d\nwant: %v\ngot: %v\n",
								i+1, test.desc, j+1, &subtest.want, fwdPub)
						}
				}
			}
			test.broker.abFwdChansMutex.RUnlock()
		}
	}
}

type abProcessingTest struct {
	pubReq pb.AbPubRequest
	want pb.AbFwdPublication
}

var message1 = "Some data."
var message2 = "Some other data."
var message3 = "Some more data."
var message4 = "Even more data."

func Equals(a pb.AbFwdPublication, b pb.AbFwdPublication) bool {
	if a.PublisherID != b.PublisherID {
		return false
	} else if a.PublicationID != b.PublicationID {
		return false
	} else if a.BrokerID != b.BrokerID {
		return false
	} else if !bytes.Equal(a.Publication, b.Publication) {
		return false
	}
	return true
}

var abProcessingTests = []struct {
	broker *Broker
	desc string
	numSubscribers int
	subtests []abProcessingTest
}{
	{
		broker: NewBroker(),
		desc: "1 pub request, 1 subscriber",
		numSubscribers: 1,
		subtests: []abProcessingTest{
			{
				pubReq: pb.AbPubRequest{
					PublisherID: 1,
					PublicationID: 1,
					Publication: []byte(message1),
				},
				want: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 0,
					Publication: []byte(message1),
				},
			},
		},
	},
	{
		broker: NewBroker(),
		desc: "1 pub request, 3 subscribers",
		numSubscribers: 3,
		subtests: []abProcessingTest{
			{
				pubReq: pb.AbPubRequest{
					PublisherID: 1,
					PublicationID: 1,
					Publication: []byte(message1),
				},
				want: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 0,
					Publication: []byte(message1),
				},
			},
		},
	},
	{
		broker: NewBroker(),
		desc: "5 pub requests, 3 subscribers",
		numSubscribers: 3,
		subtests: []abProcessingTest{
			{
				pubReq: pb.AbPubRequest{
					PublisherID: 1,
					PublicationID: 1,
					Publication: []byte(message1),
				},
				want: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 0,
					Publication: []byte(message1),
				},
			},
			{
				pubReq: pb.AbPubRequest{
					PublisherID: 1,
					PublicationID: 2,
					Publication: []byte(message2),
				},
				want: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 2,
					BrokerID: 0,
					Publication: []byte(message2),
				},
			},
			{
				pubReq: pb.AbPubRequest{
					PublisherID: 2,
					PublicationID: 1,
					Publication: []byte(message3),
				},
				want: pb.AbFwdPublication{
					PublisherID: 2,
					PublicationID: 1,
					BrokerID: 0,
					Publication: []byte(message3),
				},
			},
			{
				pubReq: pb.AbPubRequest{
					PublisherID: 2,
					PublicationID: 2,
					Publication: []byte(message4),
				},
				want: pb.AbFwdPublication{
					PublisherID: 2,
					PublicationID: 2,
					BrokerID: 0,
					Publication: []byte(message4),
				},
			},
			{
				pubReq: pb.AbPubRequest{
					PublisherID: 3,
					PublicationID: 1,
					Publication: []byte(message1),
				},
				want: pb.AbFwdPublication{
					PublisherID: 3,
					PublicationID: 1,
					BrokerID: 0,
					Publication: []byte(message1),
				},
			},
		},
	},
}