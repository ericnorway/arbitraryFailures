package main

import (
	"bytes"
	"testing"
	"time"
	pb "github.com/ericnorway/arbitraryFailures/abTemp/proto"
)

func TestCheckQuorum(t *testing.T) {
	for i, test := range quorumTests {
		for j, subtest := range test.subtests {
			if test.subscriber.pubsReceived[subtest.fwdPub.PublisherID] == nil {
				test.subscriber.pubsReceived[subtest.fwdPub.PublisherID] = make(map[int64] map[int64] []byte)
			}
			if test.subscriber.pubsReceived[subtest.fwdPub.PublisherID][subtest.fwdPub.PublicationID] == nil {
				test.subscriber.pubsReceived[subtest.fwdPub.PublisherID][subtest.fwdPub.PublicationID] = make(map[int64] []byte)
			}
			if test.subscriber.pubsReceived[subtest.fwdPub.PublisherID][subtest.fwdPub.PublicationID][subtest.fwdPub.BrokerID] == nil {
				test.subscriber.pubsReceived[subtest.fwdPub.PublisherID][subtest.fwdPub.PublicationID][subtest.fwdPub.BrokerID] = subtest.fwdPub.Publication
			}
			
			// Check that the subscriber only learns when a quorum is reached.
			result := test.subscriber.checkQuorum(subtest.fwdPub.PublisherID, subtest.fwdPub.PublicationID, subtest.quorumSize)
			if result != subtest.wantLearned {
				t.Errorf("CheckQuorum\ntest nr:%d\ndescription: %s\naction nr: %d\nwant: %v\ngot: %v\n",
						i+1, test.desc, j+1, subtest.wantLearned, result)
			}
			
			// Check that the slearned value is correct.
			msg, exists := test.subscriber.pubsLearned[subtest.fwdPub.PublisherID][subtest.fwdPub.PublicationID]
			if exists {
				if !bytes.Equal(msg, subtest.wantMessage) {
					t.Errorf("CheckQuorum\ntest nr:%d\ndescription: %s\naction nr: %d\nwant: %v\ngot: %v\n",
						i+1, test.desc, j+1, subtest.wantMessage, msg)
				}
			}
		}
	}
}

type quorumTest struct {
	fwdPub pb.AbFwdPublication
	quorumSize int
	wantLearned bool
	wantMessage []byte
}

var message1 = "Some data."
var message2 = "Some other data."
var message3 = "Some more data."
var message4 = "Even more data."

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
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte(message1),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: nil,
			},
		},
	},
	{
		NewSubscriber(),
		"1 publisher, 1 publication, 1 broker, received twice",
		[]quorumTest{
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte(message1),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: nil,
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte(message1),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: nil,
			},
		},
	},
	{
		NewSubscriber(),
		"1 publisher, 1 publication, 2 brokers",
		[]quorumTest{
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte(message1),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: nil,
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 2,
					Publication: []byte(message1),
				},
				quorumSize: 2,
				wantLearned: true,
				wantMessage: []byte(message1),
			},
		},
	},
	{
		NewSubscriber(),
		"1 publisher, 1 publication, 3 brokers",
		[]quorumTest{
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte(message1),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: nil,
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 2,
					Publication: []byte(message1),
				},
				quorumSize: 2,
				wantLearned: true,
				wantMessage: []byte(message1),
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 3,
					Publication: []byte(message1),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: []byte(message1),
			},
		},
	},
	{
		NewSubscriber(),
		"1 publisher, 1 publication, 4 brokers, quorum 3",
		[]quorumTest{
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte(message1),
				},
				quorumSize: 3,
				wantLearned: false,
				wantMessage: nil,
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 2,
					Publication: []byte(message1),
				},
				quorumSize: 3,
				wantLearned: false,
				wantMessage: nil,
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 3,
					Publication: []byte(message1),
				},
				quorumSize: 3,
				wantLearned: true,
				wantMessage: []byte(message1),
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 4,
					Publication: []byte(message1),
				},
				quorumSize: 3,
				wantLearned: false,
				wantMessage: []byte(message1),
			},
		},
	},
	{
		NewSubscriber(),
		"1 publisher, 1 publication, 5 brokers, quorum 4",
		[]quorumTest{
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte(message1),
				},
				quorumSize: 4,
				wantLearned: false,
				wantMessage: nil,
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 2,
					Publication: []byte(message1),
				},
				quorumSize: 4,
				wantLearned: false,
				wantMessage: nil,
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 3,
					Publication: []byte(message1),
				},
				quorumSize: 4,
				wantLearned: false,
				wantMessage: nil,
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 4,
					Publication: []byte(message1),
				},
				quorumSize: 4,
				wantLearned: true,
				wantMessage: []byte(message1),
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 5,
					Publication: []byte(message1),
				},
				quorumSize: 4,
				wantLearned: false,
				wantMessage: []byte(message1),
			},
		},
	},
	{
		NewSubscriber(),
		"1 publisher, 1 publication (first and second the same), 3 brokers",
		[]quorumTest{
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte(message1),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: nil,
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 2,
					Publication: []byte(message1),
				},
				quorumSize: 2,
				wantLearned: true,
				wantMessage: []byte(message1),
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 3,
					Publication: []byte(message2),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: []byte(message1),
			},
		},
	},
	{
		NewSubscriber(),
		"1 publisher, 1 publication (first and last the same), 3 brokers",
		[]quorumTest{
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte(message1),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: nil,
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 2,
					Publication: []byte(message2),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: nil,
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 3,
					Publication: []byte(message1),
				},
				quorumSize: 2,
				wantLearned: true,
				wantMessage: []byte(message1),
			},
		},
	},
	{
		NewSubscriber(),
		"1 publisher, 1 publication (second and last the same), 3 brokers",
		[]quorumTest{
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte(message1),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: nil,
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 2,
					Publication: []byte(message2),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: nil,
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 3,
					Publication: []byte(message2),
				},
				quorumSize: 2,
				wantLearned: true,
				wantMessage: []byte(message2),
			},
		},
	},
	{
		NewSubscriber(),
		"1 publisher, 1 publication (all different), 3 brokers",
		[]quorumTest{
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte(message1),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: nil,
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 2,
					Publication: []byte(message2),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: nil,
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 3,
					Publication: []byte(message3),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: nil,
			},
		},
	},
	{
		NewSubscriber(),
		"1 publisher, 2 publications, 3 brokers",
		[]quorumTest{
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte(message1),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: nil,
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 2,
					Publication: []byte(message1),
				},
				quorumSize: 2,
				wantLearned: true,
				wantMessage: []byte(message1),
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 3,
					Publication: []byte(message1),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: []byte(message1),
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 2,
					BrokerID: 1,
					Publication: []byte(message2),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: nil,
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 2,
					BrokerID: 2,
					Publication: []byte(message2),
				},
				quorumSize: 2,
				wantLearned: true,
				wantMessage: []byte(message2),
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 2,
					BrokerID: 3,
					Publication: []byte(message2),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: []byte(message2),
			},
		},
	},
	{
		NewSubscriber(),
		"3 publishers, several publications, 3 brokers",
		[]quorumTest{
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte(message1),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: nil,
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 3,
					Publication: []byte(message1),
				},
				quorumSize: 2,
				wantLearned: true,
				wantMessage: []byte(message1),
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 2,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte(message2),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: nil,
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 2,
					Publication: []byte(message1),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: []byte(message1),
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 2,
					PublicationID: 1,
					BrokerID: 2,
					Publication: []byte(message2),
				},
				quorumSize: 2,
				wantLearned: true,
				wantMessage: []byte(message2),
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 2,
					BrokerID: 1,
					Publication: []byte(message4),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: nil,
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 3,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte(message3),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: nil,
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 2,
					PublicationID: 1,
					BrokerID: 3,
					Publication: []byte(message2),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: []byte(message2),
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 2,
					BrokerID: 2,
					Publication: []byte(message4),
				},
				quorumSize: 2,
				wantLearned: true,
				wantMessage: []byte(message4),
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 3,
					PublicationID: 1,
					BrokerID: 2,
					Publication: []byte(message3),
				},
				quorumSize: 2,
				wantLearned: true,
				wantMessage: []byte(message3),
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 3,
					PublicationID: 1,
					BrokerID: 3,
					Publication: []byte(message3),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: []byte(message3),
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 2,
					BrokerID: 3,
					Publication: []byte(message4),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: []byte(message4),
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 3,
					PublicationID: 2,
					BrokerID: 3,
					Publication: []byte(message1),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: nil,
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 3,
					PublicationID: 2,
					BrokerID: 2,
					Publication: []byte(message1),
				},
				quorumSize: 2,
				wantLearned: true,
				wantMessage: []byte(message1),
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 3,
					BrokerID: 1,
					Publication: []byte(message1),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: nil,
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 3,
					BrokerID: 2,
					Publication: []byte(message1),
				},
				quorumSize: 2,
				wantLearned: true,
				wantMessage: []byte(message1),
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 3,
					PublicationID: 2,
					BrokerID: 1,
					Publication: []byte(message1),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: []byte(message1),
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 3,
					BrokerID: 3,
					Publication: []byte(message1),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: []byte(message1),
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 4,
					BrokerID: 1,
					Publication: []byte(message1),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: nil,
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 4,
					BrokerID: 2,
					Publication: []byte(message2),
				},
				quorumSize: 2,
				wantLearned: false,
				wantMessage: nil,
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 4,
					BrokerID: 3,
					Publication: []byte(message1),
				},
				quorumSize: 2,
				wantLearned: true,
				wantMessage: []byte(message1),
			},
		},
	},
}

func TestProcessPublications(t *testing.T) {
	for i, test := range processTests {
		go test.subscriber.ProcessPublications()	
	
		for j, subtest := range test.subtests {
			test.subscriber.abFwdChan <- &subtest.fwdPub
			// Give ProcessPublications() time to process the publication.
			time.Sleep(1 * time.Millisecond)
			
			// Check that the learned value is correct.
			msg, exists := test.subscriber.pubsLearned[subtest.fwdPub.PublisherID][subtest.fwdPub.PublicationID]
			if exists {
				if !bytes.Equal(msg, subtest.wantMessage) {
					t.Errorf("CheckQuorum\ntest nr:%d\ndescription: %s\naction nr: %d\nwant: %v\ngot: %v\n",
						i+1, test.desc, j+1, subtest.wantMessage, msg)
				}
			}
		}
	}
}

type processTest struct {
	fwdPub pb.AbFwdPublication
	wantMessage []byte
}

var processTests = []struct {
	subscriber *Subscriber
	desc string
	subtests []processTest
}{
	{
		NewSubscriber(),
		"1 publisher, 1 publication, 1 broker",
		[]processTest{
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte(message1),
				},
				wantMessage: nil,
			},
		},
	},
	{
		NewSubscriber(),
		"1 publisher, 1 publication, 1 broker, received twice",
		[]processTest{
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte(message1),
				},
				wantMessage: nil,
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte(message1),
				},
				wantMessage: nil,
			},
		},
	},
	{
		NewSubscriber(),
		"1 publisher, 1 publication, 2 brokers",
		[]processTest{
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte(message1),
				},
				wantMessage: nil,
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 2,
					Publication: []byte(message1),
				},
				wantMessage: []byte(message1),
			},
		},
	},
	{
		NewSubscriber(),
		"1 publisher, 1 publication, 3 brokers",
		[]processTest{
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte(message1),
				},
				wantMessage: nil,
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 2,
					Publication: []byte(message1),
				},
				wantMessage: []byte(message1),
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 3,
					Publication: []byte(message1),
				},
				wantMessage: []byte(message1),
			},
		},
	},
	{
		NewSubscriber(),
		"1 publisher, 1 publication (first and second the same), 3 brokers",
		[]processTest{
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 1,
					Publication: []byte(message1),
				},
				wantMessage: nil,
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 2,
					Publication: []byte(message1),
				},
				wantMessage: []byte(message1),
			},
			{
				fwdPub: pb.AbFwdPublication{
					PublisherID: 1,
					PublicationID: 1,
					BrokerID: 3,
					Publication: []byte(message2),
				},
				wantMessage: []byte(message1),
			},
		},
	},
}