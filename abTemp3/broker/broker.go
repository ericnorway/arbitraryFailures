package main

import (
	"fmt"
	"io"
	"net"
	"sync"
	
	"github.com/ericnorway/arbitraryFailures/abTemp3/common"
	pb "github.com/ericnorway/arbitraryFailures/abTemp3/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

type Broker struct {
	pubChan chan *pb.Publication
	subChansMutex sync.RWMutex
	subChans map[string] chan *pb.Publication
	forwardedPub map[int64] map[int64] bool
}

func NewBroker() *Broker {
	return &Broker{
		pubChan: make(chan *pb.Publication),
		subChans: make(map[string] chan *pb.Publication),
		forwardedPub: make(map[int64] map[int64] bool),
	}
}

func StartBroker(endpoint string) {
	fmt.Printf("Broker started.\n")

	listener, err := net.Listen("tcp", endpoint)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Printf("Listener started on %v\n", endpoint)
	}
	
	broker := NewBroker()
	
	grpcServer := grpc.NewServer()
	pb.RegisterPubBrokerServer(grpcServer, broker)
	pb.RegisterSubBrokerServer(grpcServer, broker)
	go broker.handleMessages()
	
	fmt.Printf("Preparing to serve incoming requests.\n")
	err = grpcServer.Serve(listener)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

func (b *Broker) addSubChannel(addr string) chan *pb.Publication {
	fmt.Printf("Subscriber channel to %v added.\n", addr)
	
	b.subChansMutex.Lock()
	defer b.subChansMutex.Unlock()
	
	ch := make(chan *pb.Publication, 32)
	b.subChans[addr] = ch
	return ch
}

func (b *Broker) removeSubChannel(addr string) {
	fmt.Printf("Subscriber channel to %v removed.\n", addr)

	b.subChansMutex.Lock()
	b.subChansMutex.Unlock()
	
	delete(b.subChans, addr)
}

func (b *Broker) Publish(stream pb.PubBroker_PublishServer) error {
	// Write loop
	go func() {
		for {
			// Nothing to write yet
		}
	} ()

	// Read loop.
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return err
		} else if err != nil {
			return err
		}
		
		// Add pub req for processing
		b.pubChan<- req
	}
	return nil
}

func (b *Broker) Subscribe(stream pb.SubBroker_SubscribeServer) error {
	pr, _ := peer.FromContext(stream.Context())
	addr := pr.Addr.String()
	ch := b.addSubChannel(addr)
	
	// Write loop
	go func() {
		for {
			select {
				case fwd := <-ch:
					err := stream.Send(fwd)
					if err != nil {
						b.removeSubChannel(addr)
						break;
					}
			}
		}
	} ()
	
	// Read loop
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			b.removeSubChannel(addr)
			return err
		} else if err != nil {
			b.removeSubChannel(addr)
			return err
		}
	}
	
	return nil
}

func (b Broker) handleMessages() {
	for {
		select {
			case req := <-b.pubChan:
				if req.PubType == common.AB {
					b.handleAbPublish(req)
				}
		}
	}
}

func (b Broker) handleAbPublish(req *pb.Publication) {
	if b.forwardedPub[req.PublisherID] == nil {
		b.forwardedPub[req.PublisherID] = make(map[int64] bool)
	}
				
	// If this publication has not been forwared yet
	if b.forwardedPub[req.PublisherID][req.PublicationID] == false {
		req.BrokerID = int64(*brokerID)
	
		// Add the publication to all forwarding channels
		b.subChansMutex.RLock()
		for _, ch := range b.subChans {
			ch<- req
		}
		b.subChansMutex.RUnlock()
		
		// Mark this publication as sent
		b.forwardedPub[req.PublisherID][req.PublicationID] = true
	} else {
		fmt.Printf("Already forwarded publication %v by publisher %v\n", req.PublicationID, req.PublisherID)
	}
}