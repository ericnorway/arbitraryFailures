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
				case pub := <-test.subscriber.ToUserPubCh:
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
	subscriber *Subscriber
	desc       string
	subtests   []handlePublishTest
}{
	{
		subscriber: NewSubscriber(0, 4),
		desc:       "1 AB publication from 4 brokers",
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
		subscriber: NewSubscriber(0, 4),
		desc:       "2 AB publications from 4 brokers",
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
		subscriber: NewSubscriber(0, 4),
		desc:       "1 AB publication, 1 BRB from 4 brokers",
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

func TestHandleHistory(t *testing.T) {
	for i, test := range handleHistoryTests {
		go test.subscriber.handlePublications()

		for j, subtest := range test.subtests {
			// Add publication
			select {
			case test.subscriber.fromBrokerCh <- subtest.pubReq:
			}

			var got []pb.Publication

			for k := 0; k < subtest.output; k++ {
				select {
				case pub := <-test.subscriber.ToUserPubCh:
					got = append(got, pub)
				}
			}

			if len(got) != len(subtest.want) {
				t.Errorf("HandleAbPublish\ntest nr:%d\ndescription: %s\naction nr: %d\nwant: %v\ngot: %v\n",
					i+1, test.desc, j+1, subtest.want, got)
				continue
			}

			for _, wantPub := range subtest.want {
				foundMatch := false
				for _, gotPub := range got {
					foundMatch = common.Equals(wantPub, gotPub)

					if foundMatch {
						break // for _, gotPub := range got
					}
				}

				if !foundMatch {
					t.Errorf("HandleAbPublish\ntest nr:%d\ndescription: %s\naction nr: %d\nwant: %v\ngot:  %v\n",
						i+1, test.desc, j+1, subtest.want, got)
					break // for _, wantPub := range subtest.want
				}
			}
		}
	}
}

type handleHistoryTest struct {
	pubReq pb.Publication
	output int
	want   []pb.Publication
}

var handleHistoryTests = []struct {
	subscriber *Subscriber
	desc       string
	subtests   []handleHistoryTest
}{
	{
		subscriber: NewSubscriber(0, 4),
		desc:       "3 AB publications from 4 brokers\n (2nd publication missing from two brokers),\n 1 BRB history publication from 4 brokers",
		subtests: []handleHistoryTest{
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
				output: 0,
				want:   []pb.Publication{},
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
				output: 0,
				want:   []pb.Publication{},
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
				output: 1,
				want: []pb.Publication{
					pb.Publication{
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
				output: 0,
				want:   []pb.Publication{},
			},
			{
				pubReq: pb.Publication{
					PubType:       common.AB,
					PublisherID:   1,
					PublicationID: 2,
					TopicID:       1,
					BrokerID:      0,
					Contents: [][]byte{
						[]byte(common.Message3),
					},
				},
				output: 0,
				want:   []pb.Publication{},
			},
			{
				pubReq: pb.Publication{
					PubType:       common.AB,
					PublisherID:   1,
					PublicationID: 2,
					TopicID:       1,
					BrokerID:      1,
					Contents: [][]byte{
						[]byte(common.Message3),
					},
				},
				output: 0,
				want:   []pb.Publication{},
			},
			{
				pubReq: pb.Publication{
					PubType:       common.AB,
					PublisherID:   1,
					PublicationID: 3,
					TopicID:       1,
					BrokerID:      0,
					Contents: [][]byte{
						[]byte(common.Message4),
					},
				},
				output: 0,
				want:   []pb.Publication{},
			},
			{
				pubReq: pb.Publication{
					PubType:       common.AB,
					PublisherID:   1,
					PublicationID: 3,
					TopicID:       1,
					BrokerID:      1,
					Contents: [][]byte{
						[]byte(common.Message4),
					},
				},
				output: 0,
				want:   []pb.Publication{},
			},
			{
				pubReq: pb.Publication{
					PubType:       common.AB,
					PublisherID:   1,
					PublicationID: 3,
					TopicID:       1,
					BrokerID:      2,
					Contents: [][]byte{
						[]byte(common.Message4),
					},
				},
				output: 1,
				want: []pb.Publication{
					pb.Publication{
						PubType:       common.AB,
						PublisherID:   1,
						PublicationID: 3,
						TopicID:       1,
						BrokerID:      2,
						Contents: [][]byte{
							[]byte(common.Message4),
						},
					},
				},
			},
			{
				pubReq: pb.Publication{
					PubType:       common.AB,
					PublisherID:   1,
					PublicationID: 3,
					TopicID:       1,
					BrokerID:      3,
					Contents: [][]byte{
						[]byte(common.Message4),
					},
				},
				output: 0,
				want:   []pb.Publication{},
			},
			{
				pubReq: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: -1,
					TopicID:       1,
					BrokerID:      0,
					Contents: [][]byte{
						common.Message1PubID1,
						common.Message3PubID2,
						common.Message4PubID3,
					},
				},
				output: 0,
				want:   []pb.Publication{},
			},
			{
				pubReq: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: -1,
					TopicID:       1,
					BrokerID:      1,
					Contents: [][]byte{
						common.Message1PubID1,
						common.Message3PubID2,
						common.Message4PubID3,
					},
				},
				output: 0,
				want:   []pb.Publication{},
			},
			{
				pubReq: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: -1,
					TopicID:       1,
					BrokerID:      2,
					Contents: [][]byte{
						common.Message1PubID1,
						common.Message3PubID2,
						common.Message4PubID3,
					},
				},
				output: 2,
				want: []pb.Publication{
					pb.Publication{
						PubType:       common.BRB,
						PublisherID:   1,
						PublicationID: -1,
						TopicID:       1,
						BrokerID:      2,
						Contents: [][]byte{
							common.Message1PubID1,
							common.Message3PubID2,
							common.Message4PubID3,
						},
					},
					pb.Publication{
						PubType:       common.BRB,
						PublisherID:   1,
						PublicationID: 2,
						TopicID:       1,
						BrokerID:      2,
						Contents: [][]byte{
							[]byte(common.Message3),
						},
					},
				},
			},
			{
				pubReq: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   1,
					PublicationID: -1,
					TopicID:       1,
					BrokerID:      3,
					Contents: [][]byte{
						common.Message1PubID1,
						common.Message3PubID2,
						common.Message4PubID3,
					},
				},
				output: 0,
				want:   []pb.Publication{},
			},
		},
	},
}
