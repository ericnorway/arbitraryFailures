package main

import (
	"fmt"
	"io"
	"time"

	pb "github.com/ericnorway/arbitraryFailures/abTemp/proto"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func (s *Subscriber) AbStartBrokerClients(brokerAddrs []string) {
	for i := range brokerAddrs {
		go s.AbStartBrokerClient(brokerAddrs[i])
	}
	
	for len(s.abSubChans) < 3 {
		fmt.Printf("Waiting for connections...\n")
		time.Sleep(time.Second)
	}
	fmt.Printf("...done\n")
}

func (s *Subscriber) AbStartBrokerClient(brokerAddr string) bool {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	
	conn, err := grpc.Dial(brokerAddr, opts...)
	if err != nil {
		fmt.Printf("Error while connecting to server: %v\n", err)
		return false
	}

	client := pb.NewAbSubBrokerClient(conn)
	
	stream, err := client.Subscribe(context.Background())
	if err != nil {
		fmt.Printf("Error while starting the Subscribe stream: %v\n", err)
		return false
	}
	
	// Read loop
	go func() {
		for {
			fwdPub, err := stream.Recv()
			if err == io.EOF {
				fmt.Printf("AB Broker ended stream.\n")
				break		
			}
			if err != nil {
				fmt.Printf("Error while AB receiving: %v\n", err)
				break
			}
			
			s.abFwdChan <- fwdPub
		}
		
		fmt.Printf("AB Receive ended.\n")
	}()
	
	err = stream.Send(&pb.AbSubRequest {
		SubscriberID: 1,
	})
	if err != nil {
		return false
	}
	s.abAddChannel(brokerAddr)
	
	return true
}

func (s *Subscriber) abAddChannel(addr string) chan *pb.AbSubRequest {
	s.abSubChansMutex.Lock()
	defer s.abSubChansMutex.Unlock()
	
	ch := make(chan *pb.AbSubRequest, 32)
	s.abSubChans[addr] = ch
	return ch
}

func (s *Subscriber) abRemoveChannel(addr string) {
	s.abSubChansMutex.Lock()
	s.abSubChansMutex.Unlock()
	
	delete(s.abSubChans, addr)
}

func (s *Subscriber) ProcessPublications() {
	fmt.Printf("Started ProcessPublications().\n")

	for {
		select {
			case pub := <-s.abFwdChan:
				// Make the map so not trying to access nil reference
				if s.pubsReceived[pub.PublisherID] == nil {
					s.pubsReceived[pub.PublisherID] = make(map[int64] map[int64] []byte)
				}
				// Make the map so not trying to access nil reference
				if s.pubsReceived[pub.PublisherID][pub.PublicationID] == nil {
					s.pubsReceived[pub.PublisherID][pub.PublicationID] = make(map[int64] []byte)
				}
				// Publication has not been received yet for this publisher ID, publication ID, broker ID
				if s.pubsReceived[pub.PublisherID][pub.PublicationID][pub.BrokerID] == nil {
					// So record it
					s.pubsReceived[pub.PublisherID][pub.PublicationID][pub.BrokerID] = pub.Publication
					// Check if there is a quorum yet for this publisher ID and publication ID
					s.checkQuorum(pub.PublisherID, pub.PublicationID, 3)
				}
			default:
		}
	}
}

func (s *Subscriber) checkQuorum(publisherID int64, publicationID int64, quorumSize int) bool {
	// It's nil, so nothing to check.
	if s.pubsReceived[publisherID] == nil {
		s.pubsReceived[publisherID] = make(map[int64] map[int64] []byte)
		return false
	}
	// It's nil, so nothing to check.
	if s.pubsReceived[publisherID][publicationID] == nil {
		s.pubsReceived[publisherID][publicationID] = make(map[int64] []byte)
		return false
	}
	
	// Make the map so not trying to access nil reference
	if s.pubsLearned[publisherID] == nil {
		s.pubsLearned[publisherID] = make(map[int64] []byte)
	}
	// If already learned this publication
	if s.pubsLearned[publisherID][publicationID] != nil {
		fmt.Printf("Already learned publication %v from publisher %v.\n", publicationID, publisherID)
		return false
	}
	
	// Just a temporary map to help with checking for a quorum. It keeps track of the number of each
	// publication value with this publisher ID and publication ID.
	countMap := make(map[string] int)
	
	for _, publication := range s.pubsReceived[publisherID][publicationID] {
		pub := string(publication)
		countMap[pub] = countMap[pub] + 1
		if countMap[pub] >= quorumSize {
			s.pubsLearned[publisherID][publicationID] = publication
			fmt.Printf("Learned publication %v from publisher %v.\n", publicationID, publisherID)
			return true
		}
	}
	
	return false
}
