package subscriber

import (
	//"fmt"
	"testing"

	"github.com/ericnorway/arbitraryFailures/common"
	pb "github.com/ericnorway/arbitraryFailures/proto"
)

func TestHandlePublications(t *testing.T) {
	for i, test := range handlePublishTests {
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

type handlePublishTest struct {
	pubReq pb.Publication
	output bool
	want   pb.Publication
}

var handlePublishTests = []struct {
	subscriber     *Subscriber
	desc           string
	numSubscribers int
	subtests       []handlePublishTest
}{
	{
		subscriber:     NewSubscriber(0),
		desc:           "1 AB publication from 4 brokers",
		numSubscribers: 1,
		subtests: []handlePublishTest{
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
	},
	{
		subscriber:     NewSubscriber(0),
		desc:           "2 AB publications from 4 brokers",
		numSubscribers: 1,
		subtests: []handlePublishTest{
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
					PublicationID: 2,
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
					PublicationID: 2,
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
			{
				pubReq: pb.Publication{
					PubType:       common.AB,
					PublisherID:   1,
					PublicationID: 2,
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
					PublicationID: 2,
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
					PublicationID: 2,
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
	},
	{
		subscriber:     NewSubscriber(0),
		desc:           "1 AB publication, 1 BRB from 4 brokers",
		numSubscribers: 1,
		subtests: []handlePublishTest{
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
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 2,
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
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 2,
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
			{
				pubReq: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 2,
					TopicID:       1,
					BrokerID:      2,
					Contents: [][]byte{
						[]byte(common.Message1),
					},
				},
				output: true,
				want: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 2,
					TopicID:       1,
					BrokerID:      2,
					Contents: [][]byte{
						[]byte(common.Message1),
					},
				},
			},
			{
				pubReq: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: 2,
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
	},
}
