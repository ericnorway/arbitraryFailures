package subscriber

import (
	//"fmt"
	"testing"

	"github.com/ericnorway/arbitraryFailures/common"
	pb "github.com/ericnorway/arbitraryFailures/proto"
)

func TestHandleABPublications(t *testing.T) {
	for i, test := range handleAbPublishTests {
		go test.subscriber.handlePublications()

		for j, subtest := range test.subtests {
			// Add publication
			select {
			case test.subscriber.fromBrokerCh <- subtest.pubReq:
			}

			if subtest.output {
				select {
				case pub := <-test.subscriber.ToUserCh:
					if !common.Equals(pub, subtest.want) {
						t.Errorf("HandleAbPublish\ntest nr:%d\ndescription: %s\naction nr: %d\nwant: %v\ngot: %v\n",
							i+1, test.desc, j+1, &subtest.want, pub)
					}
				}
			}
		}
	}
}

type handleAbPublishTest struct {
	pubReq pb.Publication
	output bool
	want   pb.Publication
}

var handleAbPublishTests = []struct {
	subscriber     *Subscriber
	desc           string
	numSubscribers int
	subtests       []handleAbPublishTest
}{
	{
		subscriber:     NewSubscriber(0),
		desc:           "1 publication from 4 brokers",
		numSubscribers: 1,
		subtests: []handleAbPublishTest{
			{
				pubReq: pb.Publication{
					PubType:       common.AB,
					PublisherID:   1,
					PublicationID: 1,
					TopicID:       1,
					BrokerID:      0,
					Contents: [][]byte{
						[]byte(common.Message1),
					},
				},
				output: false,
				want:   pb.Publication{},
			},
			{
				pubReq: pb.Publication{
					PubType:       common.AB,
					PublisherID:   1,
					PublicationID: 1,
					TopicID:       1,
					BrokerID:      1,
					Contents: [][]byte{
						[]byte(common.Message1),
					},
				},
				output: false,
				want:   pb.Publication{},
			},
			{
				pubReq: pb.Publication{
					PubType:       common.AB,
					PublisherID:   1,
					PublicationID: 1,
					TopicID:       1,
					BrokerID:      2,
					Contents: [][]byte{
						[]byte(common.Message1),
					},
				},
				output: true,
				want: pb.Publication{
					PubType:       common.AB,
					PublisherID:   1,
					PublicationID: 1,
					TopicID:       1,
					BrokerID:      2,
					Contents: [][]byte{
						[]byte(common.Message1),
					},
				},
			},
			{
				pubReq: pb.Publication{
					PubType:       common.AB,
					PublisherID:   1,
					PublicationID: 1,
					TopicID:       1,
					BrokerID:      3,
					Contents: [][]byte{
						[]byte(common.Message1),
					},
				},
				output: false,
				want:   pb.Publication{},
			},
		},
	}, /*
		{
			subscriber:         NewSubscriber(0),
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
			subscriber:         NewSubscriber(0),
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
		},*/
}
