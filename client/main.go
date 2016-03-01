package main

import (
	"crypto/hmac"
	"crypto/sha512"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"time"

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
	} else if *clientType == "subscriber" {
		mainSub()
	}
}

// mainSub starts a subscriber.
func mainSub() {
	fmt.Printf("Subscriber started.\n")
	pubTimes := make(map[int64]map[int64]int64)
	allPubIDs := make(map[int64][]int64)
	osCh := make(chan os.Signal, 1)
	signal.Notify(osCh, os.Interrupt)

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

	go func() {
		for {
			select {
			case pub := <-s.ToUser:
				if pubTimes[pub.PublisherID] == nil {
					pubTimes[pub.PublisherID] = make(map[int64]int64)
				}

				timeNano := time.Now().UnixNano()
				pubTimes[pub.PublisherID][pub.PublicationID] = timeNano
				allPubIDs[pub.PublisherID] = append(allPubIDs[pub.PublisherID], pub.PublicationID)
			}
		}
	}()

	// Wait for ctrl-c
	<-osCh

	file, err := os.Create("recvTimes" + strconv.FormatInt(localID, 10) + ".txt")
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}
	defer file.Close()

	for publisherID, pubIDs := range allPubIDs {
		// Record the times sent in a file.
		for _, pubID := range pubIDs {
			file.Write([]byte(fmt.Sprintf("%v:%v:%v:%v\n", publisherID, pubID, localID, pubTimes[publisherID][pubID])))
		}
	}
}

// mainPub starts a publisher and publishes three publications.
func mainPub() {
	fmt.Printf("Publisher started.\n")
	pubTimes := make(map[int64]int64)
	var pubIDs []int64

	p := publisher.NewPublisher(localID)

	// Add broker information
	for i, key := range brokerKeys {
		id := int64(i)
		p.AddBroker(id, brokerAddresses[id], []byte(key))
	}

	p.Start()

	mac := hmac.New(sha512.New, []byte(""))
	time.Sleep(time.Second)

	var i int64
	for i = 0; i < publicationCount; i++ {
		// Create a new random message.
		mac.Write([]byte(time.Now().String()))
		sum := mac.Sum(nil)

		// Create the publication.
		pub := &pb.Publication{
			PubType:       publicationType,
			PublisherID:   localID,
			PublicationID: i,
			Topic:         1,
			Content:       sum,
		}

		// Record the time sent in a map.
		timeNano := time.Now().UnixNano()
		pubTimes[pub.PublicationID] = timeNano
		pubIDs = append(pubIDs, pub.PublicationID)

		// Send the publication.
		p.Publish(pub)

		fmt.Printf(".")
		time.Sleep(5 * time.Millisecond)
	}

	// Make sure that the last few messages have time to be sent.
	time.Sleep(time.Second)

	file, err := os.Create("sendTimes" + strconv.FormatInt(localID, 10) + ".txt")
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}
	defer file.Close()

	// Record the times sent in a file.
	for _, pubID := range pubIDs {
		file.Write([]byte(fmt.Sprintf("%v:%v:%v\n", localID, pubID, pubTimes[pubID])))
	}
}
