package main

import (
	"fmt"
	"sync"
	"time"
	
	pb "github.com/ericnorway/arbitraryFailures/abTemp/proto"
	
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type BrokerClient struct {
	Conn grpc.ClientConn
	Stream pb.SubBrokerAB_SubscribeClient
}

type Subscriber struct {
	sync.Mutex
	BrokerClients map[string] BrokerClient
	
	forwardChan chan pb.FwdPublication

	// The first index references the publisher ID.
	// The second index references the publication ID.
	// The third index references the broker ID.
	// The byte slice contains the publication.
	pubsReceived map[int64] map[int64] map[int64] []byte
	
	// The first index references the publisher ID.
	// The second index references the publication ID.
	// The byte slice contains the publication.
	pubsLearned map[int64] map[int64] []byte
}

func NewSubscriber() *Subscriber {
	return &Subscriber{
		BrokerClients: make(map[string] BrokerClient),
		forwardChan: make(chan pb.FwdPublication, 32),
		pubsReceived: make(map[int64] map[int64] map[int64] []byte),
		pubsLearned: make(map[int64] map[int64] []byte),
	}
}

func main() {
	fmt.Printf("Subscriber started.\n")
	
	brokerAddrs := []string{"localhost:11111", "localhost:11112", "localhost:11113", "localhost:11114"}
	
	subscriber := NewSubscriber()
	
	subscriber.CreateBrokerClients(brokerAddrs)
	
	for {}
}

func (s *Subscriber) CreateBrokerClients(brokerAddrs []string) {
	for i := range brokerAddrs {
		go s.CreateBrokerClient(brokerAddrs[i])
	}
	
	for len(s.BrokerClients) < 3 {
		fmt.Printf("Waiting for connections...\n")
		time.Sleep(time.Second)
	}
}

func (s *Subscriber) CreateBrokerClient(brokerAddr string) bool {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	
	conn, err := grpc.Dial(brokerAddr, opts...)
	if err != nil {
		fmt.Printf("Error while connecting to server: %v\n", err)
		return false
	}

	client := pb.NewSubBrokerABClient(conn)
	
	stream, err := client.Subscribe(context.Background())
	if err != nil {
		fmt.Printf("Error while starting the Subscribe stream: %v\n", err)
		return false
	}
	
	s.AddClient(brokerAddr, *conn, stream)
	
	return true
}

func (s *Subscriber) AddClient(addr string, conn grpc.ClientConn, stream pb.SubBrokerAB_SubscribeClient) {
	s.Lock()
	defer s.Unlock()
	
	s.BrokerClients[addr] = BrokerClient{Conn: conn, Stream: stream}
}

func (s *Subscriber) RemoveClient(addr string) {
	s.Lock()
	s.Unlock()
	
	brokerClient, exists := s.BrokerClients[addr]
	if exists {
		fmt.Printf("Sending close.\n")
		brokerClient.Stream.CloseSend()
		fmt.Printf("Closing connection.\n")
		brokerClient.Conn.Close()
	}
	delete(s.BrokerClients, addr)
}

func (s *Subscriber) Subscribe(subReq *pb.SubRequest) {
	for i := range s.BrokerClients {
		err := s.BrokerClients[i].Stream.Send(subReq)
		if err != nil {
			fmt.Printf("Error while subscribing: %v\n", err)
			s.RemoveClient(i)
		}
		time.Sleep(time.Millisecond)
	}
}

/*func (s *Subscriber) Forward(stream pb.BrokerAC_ForwardServer) error {
	fmt.Printf("Started Forward().\n")
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			fmt.Printf("EOF\n")
			return stream.SendAndClose(&pb.ErrorMsg{
				Message: "Closed",
			})
		} else if err != nil {
			fmt.Printf("%v\n", err)
			return stream.SendAndClose(&pb.ErrorMsg{
				Message: fmt.Sprintf("%v\n", err),
			})
		}
		
		s.forwardChan <- *req
	}
	return nil
}*/

/*func (s *Subscriber) processPublications() {
	fmt.Printf("Started processPublications().\n")

	for {
		select {
			case pub := <-s.forwardChan:
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
}*/

/*func (s *Subscriber) checkQuorum(publisherID int64, publicationID int64, quorumSize int) bool {
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
}*/