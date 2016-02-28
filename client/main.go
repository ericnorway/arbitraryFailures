package main

import (
	"fmt"
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
	
	if *clientType == "publisher" {
		mainPub()
	} else if  *clientType == "subscriber" {
		mainSub()
	}
}

// mainSub starts a subscriber.
func mainSub() {
	fmt.Printf("Subscriber started.\n")

	s := subscriber.NewSubscriber(localID)

	// Add topics this subscriber is interested in.
	for _, topic := range topics {
		s.AddTopic(topic)
	}

	// Add broker information
	for i, key := range brokerKeys {
		id := int64(i)
		s.AddBroker(id, brokerAddresses[id], []byte(key))
	}

	go s.Start()

	for {
		select {
		case pub := <-s.ToUser:
			fmt.Printf("Got publication %v from publisher %v.\n", pub.PublicationID, pub.PublisherID)
		}
	}
}

// mainPub starts a publisher and publishes three publications.
func mainPub() {
	fmt.Printf("Publisher started.\n")

	p := publisher.NewPublisher(localID)

	// Add broker information
	for i, key := range brokerKeys {
		id := int64(i)
		p.AddBroker(id, brokerAddresses[id], []byte(key))
	}

	p.StartBrokerClients()

	time.Sleep(time.Second)

	p.Publish(&pb.Publication{
		PubType:       common.AB,
		PublisherID:   localID,
		PublicationID: 1,
		Topic:         1,
		Content:       []byte(time.Now().String()),
	})
	time.Sleep(time.Second)

	p.Publish(&pb.Publication{
		PubType:       common.BRB,
		PublisherID:   localID,
		PublicationID: 2,
		Topic:         2,
		Content:       []byte(time.Now().String()),
	})
	time.Sleep(time.Second)

	p.Publish(&pb.Publication{
		PubType:       common.AB,
		PublisherID:   localID,
		PublicationID: 3,
		Topic:         1,
		Content:       []byte(time.Now().String()),
	})
	time.Sleep(time.Second)
}