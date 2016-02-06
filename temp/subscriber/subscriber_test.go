package main

import (
	"testing"
	pb "github.com/ericnorway/arbitraryFailures/temp/proto"
)

func TestCheckQuorum(t *testing.T) {
	for i, test := range quorumTests {
	
		for j, subtest := range test.subtests {
			if test.subscriber.PubsReceived[subtest.fwdRequest.PublisherID] == nil {
				test.subscriber.PubsReceived[subtest.fwdRequest.PublisherID] = make(map[int64] map[int64] []byte)
			}
			if test.subscriber.PubsReceived[subtest.fwdRequest.PublisherID][subtest.fwdRequest.PublicationID] == nil {
				test.subscriber.PubsReceived[subtest.fwdRequest.PublisherID][subtest.fwdRequest.PublicationID] = make(map[int64] []byte)
			}
			if test.subscriber.PubsReceived[subtest.fwdRequest.PublisherID][subtest.fwdRequest.PublicationID][subtest.fwdRequest.BrokerID] == nil {
				test.subscriber.PubsReceived[subtest.fwdRequest.PublisherID][subtest.fwdRequest.PublicationID][subtest.fwdRequest.BrokerID] = subtest.fwdRequest.Publication
			}
				
			result := test.subscriber.CheckQuorum(subtest.fwdRequest.PublisherID, subtest.fwdRequest.PublicationID, subtest.quorumSize)
			if result != subtest.wantResult {
				t.Errorf("CheckQuorum\ntest nr:%d\ndescription: %s\naction nr: %d\nwant: %v\ngot: %v\n",
						i+1, test.desc, j+1, subtest.wantResult, result)
			}
		}
		
	}
}

type quorumTest struct {
	fwdRequest pb.FwdRequest
	quorumSize int
	wantResult bool
}

var quorumTests = []struct {
	subscriber *Subscriber
	desc string
	subtests []quorumTest
}{
	{
		NewSubscriber(),
		"1 publisher, 1 publication, 1 broker",
		[]quorumTest{
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: false,
			},
		},
	},
	{
		NewSubscriber(),
		"1 publisher, 1 publication, 1 broker, received twice",
		[]quorumTest{
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: false,
			},
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: false,
			},
		},
	},
	{
		NewSubscriber(),
		"1 publisher, 1 publication, 2 brokers",
		[]quorumTest{
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: false,
			},
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 2,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: true,
			},
		},
	},
	{
		NewSubscriber(),
		"1 publisher, 1 publication, 3 brokers",
		[]quorumTest{
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: false,
			},
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 2,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: true,
			},
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 3,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: false,
			},
		},
	},
	{
		NewSubscriber(),
		"1 publisher, 1 publication (first and last the same), 3 brokers",
		[]quorumTest{
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: false,
			},
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 2,
					Publication: []byte("Some other data."),
				},
				quorumSize: 2,
				wantResult: false,
			},
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 3,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: true,
			},
		},
	},
	{
		NewSubscriber(),
		"1 publisher, 1 publication (second and last the same), 3 brokers",
		[]quorumTest{
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: false,
			},
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 2,
					Publication: []byte("Some other data."),
				},
				quorumSize: 2,
				wantResult: false,
			},
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 3,
					Publication: []byte("Some other data."),
				},
				quorumSize: 2,
				wantResult: true,
			},
		},
	},
	{
		NewSubscriber(),
		"1 publisher, 1 publication (all different), 3 brokers",
		[]quorumTest{
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: false,
			},
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 2,
					Publication: []byte("Some other data."),
				},
				quorumSize: 2,
				wantResult: false,
			},
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 3,
					Publication: []byte("Some more data."),
				},
				quorumSize: 2,
				wantResult: false,
			},
		},
	},
	{
		NewSubscriber(),
		"1 publisher, 2 publications, 3 brokers",
		[]quorumTest{
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: false,
			},
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 2,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: true,
			},
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 3,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: false,
			},
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 1,
					PublicationID: 2,
					BrokerID: 1,
					Publication: []byte("Some other data."),
				},
				quorumSize: 2,
				wantResult: false,
			},
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 1,
					PublicationID: 2,
					BrokerID: 2,
					Publication: []byte("Some other data."),
				},
				quorumSize: 2,
				wantResult: true,
			},
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 1,
					PublicationID: 2,
					BrokerID: 3,
					Publication: []byte("Some other data."),
				},
				quorumSize: 2,
				wantResult: false,
			},
		},
	},
	{
		NewSubscriber(),
		"3 publishers, several publications, 3 brokers",
		[]quorumTest{
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: false,
			},
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 3,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: true,
			},
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 2,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: false,
			},
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 2,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: false,
			},
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 2,
					PublicationID: 1,
					BrokerID: 2,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: true,
			},
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 1,
					PublicationID: 2,
					BrokerID: 1,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: false,
			},
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 3,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: false,
			},
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 2,
					PublicationID: 1,
					BrokerID: 3,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: false,
			},
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 1,
					PublicationID: 2,
					BrokerID: 2,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: true,
			},
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 3,
					PublicationID: 1,
					BrokerID: 2,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: true,
			},
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 3,
					PublicationID: 1,
					BrokerID: 3,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: false,
			},
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 1,
					PublicationID: 2,
					BrokerID: 3,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: false,
			},
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 3,
					PublicationID: 2,
					BrokerID: 3,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: false,
			},
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 3,
					PublicationID: 2,
					BrokerID: 2,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: true,
			},
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 1,
					PublicationID: 3,
					BrokerID: 1,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: false,
			},
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 1,
					PublicationID: 3,
					BrokerID: 2,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: true,
			},
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 3,
					PublicationID: 2,
					BrokerID: 1,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: false,
			},
			{
				fwdRequest: pb.FwdRequest{
					PublisherID: 1,
					PublicationID: 3,
					BrokerID: 3,
					Publication: []byte("Some data."),
				},
				quorumSize: 2,
				wantResult: false,
			},
		},
	},
}