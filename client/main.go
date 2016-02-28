package main

import (
	"crypto/hmac"
	"crypto/sha512"
	"fmt"
	"os"
	"os/signal"
	"sort"
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
	pubs := make(map[int64]int64)
	var keys Keys
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
				timeNano := time.Now().UnixNano()
				pubs[pub.PublicationID] = timeNano
				keys = append(keys, pub.PublicationID)
			}
		}
	}()

	// Wait for ctrl-c
	<-osCh

	file, err := os.Create("recvTimes.txt")
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}
	defer file.Close()

	sort.Sort(keys)
	// Record the times sent in a file.
	for _, key := range keys {
		file.Write([]byte(fmt.Sprintf("%v:%v\n", key, pubs[key])))
	}
}

// mainPub starts a publisher and publishes three publications.
func mainPub() {
	fmt.Printf("Publisher started.\n")
	pubs := make(map[int64]int64)
	var keys Keys

	p := publisher.NewPublisher(localID)

	// Add broker information
	for i, key := range brokerKeys {
		id := int64(i)
		p.AddBroker(id, brokerAddresses[id], []byte(key))
	}

	p.StartBrokerClients()

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
			PublicationID: time.Now().Unix() + i,
			Topic:         1,
			Content:       sum,
		}

		// Record the time sent in a map.
		timeNano := time.Now().UnixNano()
		pubs[pub.PublicationID] = timeNano
		keys = append(keys, pub.PublicationID)

		// Send the publication.
		p.Publish(pub)
	}

	// Make sure that the last few messages have time to be sent.
	time.Sleep(time.Second)

	file, err := os.Create("sendTimes.txt")
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}
	defer file.Close()

	sort.Sort(keys)
	// Record the times sent in a file.
	for _, key := range keys {
		file.Write([]byte(fmt.Sprintf("%v:%v\n", key, pubs[key])))
	}
}

type Keys []int64

// Len is the number of elements in the collection.
func (k Keys) Len() int {
	return len(k)
}

// Less reports whether the element with
// index i should sort before the element with index j.
func (k Keys) Less(i, j int) bool {
	return k[i] < k[j]
}

// Swap swaps the elements with indexes i and j.
func (k Keys) Swap(i, j int) {
	k[i], k[j] = k[j], k[i]
}
