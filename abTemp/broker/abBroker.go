package main

import (
	"fmt"
	"io"
	
	pb "github.com/ericnorway/arbitraryFailures/abTemp/proto"
	"google.golang.org/grpc/peer"
)

func (b *Broker) Publish(stream pb.AbPubBroker_PublishServer) error {
	fmt.Printf("Started Publish().\n")

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			fmt.Printf("EOF\n")
			stream.Send(&pb.AbErrorMsg{
				Message: "Closed",
			})
			return err
		} else if err != nil {
			fmt.Printf("%v\n", err)
			stream.Send(&pb.AbErrorMsg{
				Message: fmt.Sprintf("%v\n", err),
			})
			return err
		}
		
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
	return nil
}

func (b *Broker) Subscribe(stream pb.AbSubBroker_SubscribeServer) error {
	fmt.Printf("Started Subscribe().\n")
	
	pr,_ := peer.FromContext(stream.Context())
	ch := b.AbAddChannel(pr.Addr.String())
	
	go func() {
		for {
			select {
				case fwd := <-ch:
					stream.Send(fwd)
			}
		}
	} ()
	
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			fmt.Printf("EOF\n")
			//stream.Send(&pb.AbFwdPublication{})
			return err
		} else if err != nil {
			fmt.Printf("%v\n", err)
			//stream.Send(&pb.AbFwdPublication{})
			return err
		}
		fmt.Printf("Subscribe received.\n")
	}
	
	return nil
}

func (b *Broker) AbAddChannel(addr string) chan *pb.AbFwdPublication {
	b.abFwdChansMutex.Lock()
	defer b.abFwdChansMutex.Unlock()
	
	ch := make(chan *pb.AbFwdPublication, 32)
	b.abFwdChans[addr] = ch
	return ch
}

func (b *Broker) AbRemoveChannel(addr string) {
	b.abFwdChansMutex.Lock()
	b.abFwdChansMutex.Unlock()
	
	delete(b.abFwdChans, addr)
}