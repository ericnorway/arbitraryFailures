package main

import (
	"crypto/hmac"
	"crypto/sha512"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/ericnorway/arbitraryFailures/common"
	pb "github.com/ericnorway/arbitraryFailures/proto"
	"github.com/ericnorway/arbitraryFailures/publisher"
	"github.com/ericnorway/arbitraryFailures/subscriber"
)

func main() {
	parsedCorrectly := ParseArgs()
	if !parsedCorrectly {
		return
	}

	err := ReadConfigFile(*configFile)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}

	numberOfBrokers := uint64(len(brokerAddresses))

	if *clientType == "publisher" {
		p := NewPublisherInstance(localID, numberOfBrokers)
		p.mainPub()
	} else if *clientType == "subscriber" {
		s := NewSubscriberInstance(localID, numberOfBrokers)
		s.mainSub()
	}
}

// SubscriberInstance ...
type SubscriberInstance struct {
	id            uint64
	subscriber    *subscriber.Subscriber
	recordTimesCh chan common.RecordTime
}

// NewSubscriberInstance ...
func NewSubscriberInstance(id uint64, numberOfBrokers uint64) *SubscriberInstance {
	return &SubscriberInstance{
		id:            localID,
		subscriber:    subscriber.NewSubscriber(id, numberOfBrokers),
		recordTimesCh: make(chan common.RecordTime, 16),
	}
}

// mainSub starts a subscriber.
func (s *SubscriberInstance) mainSub() {
	fmt.Printf("Subscriber started.\n")

	go s.RecordRecvTimes()

	// Add topics this subscriber is interested in.
	for _, topic := range topics {
		s.subscriber.AddTopic(topic)
	}

	// Add broker information
	for i, key := range brokerKeys {
		id := uint64(i)
		s.subscriber.AddBroker(id, brokerAddresses[id], []byte(key))
	}

	// Add the chain path
	s.subscriber.AddChainPath(rChain)

	go s.subscriber.Start()

	for {
		select {
		case pub := <-s.subscriber.ToUserPubCh:
			// Record the time received
			s.recordTimesCh <- common.RecordTime{
				PublisherID:   pub.PublisherID,
				PublicationID: pub.PublicationID,
				Time:          time.Now().UnixNano(),
			}
		}
	}
}

// RecordRecvTimes ...
func (s *SubscriberInstance) RecordRecvTimes() {
	file, err := os.Create("./results/recvTimes" + strconv.FormatUint(s.id, 10) + ".txt")
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}
	defer file.Close()

	for {
		select {
		case pub := <-s.recordTimesCh:
			file.Write([]byte(fmt.Sprintf("%v:%v:%v:%v\n", pub.PublisherID, pub.PublicationID, s.id, pub.Time)))
		}
	}
}

// PublisherInstance ...
type PublisherInstance struct {
	id            uint64
	publisher     *publisher.Publisher
	recordTimesCh chan common.RecordTime
}

// NewPublisherInstance ...
func NewPublisherInstance(id uint64, numberOfBrokers uint64) *PublisherInstance {
	return &PublisherInstance{
		id:            id,
		publisher:     publisher.NewPublisher(id, numberOfBrokers),
		recordTimesCh: make(chan common.RecordTime, 64),
	}
}

// mainPub starts a publisher and publishes three publications.
func (p *PublisherInstance) mainPub() {
	fmt.Printf("Publisher started.\n")

	go p.RecordSendTimes()

	// Add broker information
	for i, key := range brokerKeys {
		id := uint64(i)
		p.publisher.AddBroker(id, brokerAddresses[id], []byte(key))
	}

	// Add the chain path
	p.publisher.AddChainPath(chain)

	p.publisher.Start()

	mac := hmac.New(sha512.New, []byte(""))
	time.Sleep(time.Second)

	topicsRange := len(topics)
	random := rand.New(rand.NewSource(time.Now().Unix()))

	for i := int64(0); i < *pubCount; i++ {
		// Create a new random message.
		mac.Write([]byte("Test publication " + time.Now().String()))
		sum := mac.Sum(nil)

		currentTopic := topics[random.Intn(topicsRange)]

		// Create the publication.
		pub := &pb.Publication{
			PubType:       publicationType,
			PublisherID:   p.id,
			PublicationID: i,
			TopicID:       currentTopic,
			Contents: [][]byte{
				sum,
			},
		}

		// Record the time sent
		p.recordTimesCh <- common.RecordTime{
			PublisherID:   pub.PublisherID,
			PublicationID: pub.PublicationID,
			Time:          time.Now().UnixNano(),
		}

		for sent := false; sent == false; {
			// Send the publication.
			sent = p.publisher.Publish(pub)
			time.Sleep(5 * time.Millisecond)
		}
		fmt.Printf(".")
	}

	// Make sure that the last few messages have time to be sent.
	time.Sleep(time.Second)
}

// RecordSendTimes ...
func (p *PublisherInstance) RecordSendTimes() {
	file, err := os.Create("./results/sendTimes" + strconv.FormatUint(p.id, 10) + ".txt")
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}
	defer file.Close()

	for {
		select {
		case pub := <-p.recordTimesCh:
			file.Write([]byte(fmt.Sprintf("%v:%v:%v\n", p.id, pub.PublicationID, pub.Time)))
		case pub := <-p.publisher.ToUserRecordCh:
			file.Write([]byte(fmt.Sprintf("%v:%v:%v\n", p.id, pub.PublicationID, pub.Time)))
		}
	}
}
