package main

import (
	"fmt"
	"io"
	
	pb "github.com/ericnorway/arbitraryFailures/abTemp/proto"
	"google.golang.org/grpc/peer"
)

func (b *Broker) abAddChannel(addr string) chan *pb.AbFwdPublication {
	b.abFwdChansMutex.Lock()
	defer b.abFwdChansMutex.Unlock()
	
	ch := make(chan *pb.AbFwdPublication, 32)
	b.abFwdChans[addr] = ch
	return ch
}

func (b *Broker) abRemoveChannel(addr string) {
	b.abFwdChansMutex.Lock()
	b.abFwdChansMutex.Unlock()
	
	delete(b.abFwdChans, addr)
}

func (b *Broker) Publish(stream pb.AbPubBroker_PublishServer) error {
	fmt.Printf("Started Publish().\n")
	
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
			fmt.Printf("EOF\n")
			return err
		} else if err != nil {
			fmt.Printf("%v\n", err)
			return err
		}
		
		// Add pub req for processing
		b.abPubChan<- req
	}
	return nil
}

func (b *Broker) Subscribe(stream pb.AbSubBroker_SubscribeServer) error {
	fmt.Printf("Started Subscribe().\n")
	
	pr, _ := peer.FromContext(stream.Context())
	addr := pr.Addr.String()
	ch := b.abAddChannel(addr)
	
	// Write loop
	go func() {
		for {
			select {
				case fwd := <-ch:
					err := stream.Send(fwd)
					if err != nil {
						fmt.Printf("Error while forwarding message to subscriber: %v\n", err)
						break;
					}
			}
		}
	} ()
	
	// Read loop
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			fmt.Printf("EOF\n")
			b.abRemoveChannel(addr)
			return err
		} else if err != nil {
			fmt.Printf("%v\n", err)
			b.abRemoveChannel(addr)
			return err
		}
		fmt.Printf("Subscribe received.\n")
	}
	
	return nil
}

func (b Broker) processAbMessages() {
	for {
		select {
			case req := <-b.abPubChan:
				fwdReq := pb.AbFwdPublication{
					PublisherID: req.PublisherID,
					PublicationID: req.PublicationID,
					BrokerID: int64(*brokerID),
					Publication: req.Publication,
				}
				
				// Add the publication to all forwarding channels
				b.abFwdChansMutex.RLock()
				for _, ch := range b.abFwdChans {
					ch<- &fwdReq
				}
				b.abFwdChansMutex.RUnlock()
		
				// Mark this publication as sent
				if b.forwardedPub[req.PublisherID] == nil {
					b.forwardedPub[req.PublisherID] = make(map[int64] bool)
				}
				b.forwardedPub[req.PublisherID][req.PublicationID] = true
		}
	}
}