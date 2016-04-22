package publisher

import (
	"testing"

	"github.com/ericnorway/arbitraryFailures/common"
	pb "github.com/ericnorway/arbitraryFailures/proto"
)

func TestPublish(t *testing.T) {
	for i, test := range publishTests {

		// Manually add broker channels
		for j := 0; j < test.numBrokers; j++ {
			test.publisher.AddBroker(uint64(j), "", []byte("12345"))
			test.publisher.addChannel(uint64(j))
		}

		for j, subtest := range test.subtests {
			// Not actually going to get responses from publishers, so just preload the accept channel
			for k := 0; k < test.numBrokers; k++ {
				test.publisher.statusCh <- pb.PubResponse_OK
			}

			// Add publication request
			test.publisher.Publish(&subtest.pub)

			// Check that all "brokers" got the publication
			for _, broker := range test.publisher.brokers {
				select {
				case pub := <-broker.toCh:
					if !common.Equals(pub, subtest.want) {
						t.Errorf("TestPublish\ntest nr:%d\ndescription: %s\naction nr: %d\nwant: %v\ngot: %v\n",
							i+1, test.desc, j+1, subtest.want, pub)
					}
				}
			}
		}
	}
}

type publishTest struct {
	pub  pb.Publication
	want pb.Publication
}

var publishTests = []struct {
	publisher  *Publisher
	desc       string
	numBrokers int
	subtests   []publishTest
}{
	{
		publisher:  NewPublisher(0, 4),
		desc:       "1 AB publication",
		numBrokers: 4,
		subtests: []publishTest{
			{
				pub: pb.Publication{
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
					Contents: [][]byte{
						[]byte(common.Message1),
					},
				},
			},
		},
	},
	{
		publisher:  NewPublisher(0, 4),
		desc:       "1 BRB publication",
		numBrokers: 4,
		subtests: []publishTest{
			{
				pub: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   2,
					PublicationID: 3,
					TopicID:       4,
					Contents: [][]byte{
						[]byte(common.Message2),
					},
				},
				want: pb.Publication{
					PubType:       common.BRB,
					PublisherID:   2,
					PublicationID: 3,
					TopicID:       4,
					Contents: [][]byte{
						[]byte(common.Message2),
					},
				},
			},
		},
	},
}

func TestHistory(t *testing.T) {
	for i, test := range historyTests {

		// Manually add broker channels
		for j := 0; j < test.numBrokers; j++ {
			test.publisher.AddBroker(uint64(j), "", []byte("12345"))
			test.publisher.addChannel(uint64(j))
		}

		for j, subtest := range test.subtests {
			timeForHistory := false
			if j == len(test.subtests)-1 {
				timeForHistory = true
			}

			// Not actually going to get responses from publishers, so just preload the status channel
			if timeForHistory {
				for k := 0; k < test.numBrokers; k++ {
					test.publisher.statusCh <- pb.PubResponse_HISTORY
				}

				// Preload the status channel with history responses when the channel is available
				go func() {
					for k := 0; k < test.numBrokers; k++ {
						test.publisher.statusCh <- pb.PubResponse_OK
					}
				}()

			} else {
				for k := 0; k < test.numBrokers; k++ {
					test.publisher.statusCh <- pb.PubResponse_OK
				}
			}

			// Add publication request
			test.publisher.Publish(&subtest.pub)

			// Check that all "brokers" got the publication
			test.publisher.brokersMutex.RLock()
			for _, broker := range test.publisher.brokers {
				select {
				case pub := <-broker.toCh:
					if !common.Equals(pub, subtest.want) {
						t.Errorf("TestPublish\ntest nr:%d\ndescription: %s\naction nr: %d\nwant: %v\ngot: %v\n",
							i+1, test.desc, j+1, subtest.want, pub)
					}
				}
			}
			test.publisher.brokersMutex.RUnlock()
		}

		test.publisher.brokersMutex.RLock()
		for _, broker := range test.publisher.brokers {
			select {
			case pub := <-broker.toCh:
				if !common.Equals(pub, test.wantHistory) {
					t.Errorf("TestPublish\ntest nr:%d\ndescription: %s\nwant: %v\ngot: %v\n",
						i+1, test.desc, test.wantHistory, pub)
				}
			}
		}
		test.publisher.brokersMutex.RUnlock()
	}
}

type historyTest struct {
	pub  pb.Publication
	want pb.Publication
}

var historyTests = []struct {
	publisher   *Publisher
	desc        string
	numBrokers  int
	subtests    []historyTest
	wantHistory pb.Publication
}{
	{
		publisher:  NewPublisher(1, 4),
		desc:       "3 AB publications, history request",
		numBrokers: 4,
		subtests: []historyTest{
			{
				pub: pb.Publication{
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
					Contents: [][]byte{
						[]byte(common.Message1),
					},
				},
			},
			{
				pub: pb.Publication{
					PubType:       common.AB,
					PublisherID:   1,
					PublicationID: 2,
					TopicID:       1,
					Contents: [][]byte{
						[]byte(common.Message3),
					},
				},
				want: pb.Publication{
					PubType:       common.AB,
					PublisherID:   1,
					PublicationID: 2,
					TopicID:       1,
					Contents: [][]byte{
						[]byte(common.Message3),
					},
				},
			},
			{
				pub: pb.Publication{
					PubType:       common.AB,
					PublisherID:   1,
					PublicationID: 3,
					TopicID:       1,
					Contents: [][]byte{
						[]byte(common.Message4),
					},
				},
				want: pb.Publication{
					PubType:       common.AB,
					PublisherID:   1,
					PublicationID: 3,
					TopicID:       1,
					Contents: [][]byte{
						[]byte(common.Message4),
					},
				},
			},
		},
		wantHistory: pb.Publication{
			PubType:       common.BRB,
			PublisherID:   1,
			PublicationID: -1,
			TopicID:       1,
			Contents: [][]byte{
				common.Message1PubID1,
				common.Message3PubID2,
				common.Message4PubID3,
			},
		},
	},
}
