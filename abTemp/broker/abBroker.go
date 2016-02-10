package main

import (
	"fmt"
	"io"
	
	pb "github.com/ericnorway/arbitraryFailures/abTemp/proto"
	"google.golang.org/grpc/peer"
)

func (b *Broker) abAddChannel(addr string) chan *pb.AbFwdPublication {
	fmt.Printf("AB forward channel to %v added.\n", addr)
	
	b.abFwdChansMutex.Lock()
	defer b.abFwdChansMutex.Unlock()
	
	ch := make(chan *pb.AbFwdPublication, 32)
	b.abFwdChans[addr] = ch
	return ch
}

func (b *Broker) abRemoveChannel(addr string) {
	fmt.Printf("AB forward channel to %v removed.\n", addr)

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
			return err
		} else if err != nil {
			return err
		}
		
		// Add pub req for processing
		b.abPubChan<- req
	}
	return nil
}

func (b *Broker) Subscribe(stream pb.AbSubBroker_SubscribeServer) error {
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
						b.abRemoveChannel(addr)
						break;
					}
			}
		}
	} ()
	
	// Read loop
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			b.abRemoveChannel(addr)
			return err
		} else if err != nil {
			b.abRemoveChannel(addr)
			return err
		}
	}
	
	return nil
}

func (b Broker) processAbMessages() {
	for {
		select {
			case req := <-b.abPubChan:
				if b.abForwardedPub[req.PublisherID] == nil {
					b.abForwardedPub[req.PublisherID] = make(map[int64] bool)
				}
				
				// If this publication has not been forwared yet
				if b.abForwardedPub[req.PublisherID][req.PublicationID] == false {
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
					b.abForwardedPub[req.PublisherID][req.PublicationID] = true
				} else {
					fmt.Printf("Already forwarded publication %v by publisher %v\n", req.PublicationID, req.PublisherID)
				}
		}
	}
}