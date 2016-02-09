package main

import (
	"fmt"
	"io"
	"time"

	pb "github.com/ericnorway/arbitraryFailures/abTemp/proto"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func (p *Publisher) AbPublish(pubReq *pb.AbPubRequest) {
	p.abPubChansMutex.RLock()
	defer p.abPubChansMutex.RUnlock()
	
	for _, ch := range p.abPubChans {
		ch<- pubReq
	}
}

func (p *Publisher) AbStartBrokerClients(brokerAddrs []string) {
	for i := range brokerAddrs {
		go p.AbStartBrokerClient(brokerAddrs[i])
	}
	
	for len(p.abPubChans) < 3 {
		fmt.Printf("Waiting for connections...\n")
		time.Sleep(time.Second)
	}
	fmt.Printf("...done\n")
}

func (p *Publisher) AbStartBrokerClient(brokerAddr string) bool {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	
	conn, err := grpc.Dial(brokerAddr, opts...)
	if err != nil {
		fmt.Printf("Error while connecting to server: %v\n", err)
		return false
	}

	client := pb.NewAbPubBrokerClient(conn)
	
	stream, err := client.Publish(context.Background())
	if err != nil {
		fmt.Printf("Error while starting the Publish stream: %v\n", err)
		return false
	}
	
	// Read loop
	go func() {
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				fmt.Printf("AB Broker ended stream.\n")
				break		
			}
			if err != nil {
				fmt.Printf("Error while AB receiving: %v\n", err)
				break
			}
		}
		
		fmt.Printf("AB Receive ended.\n")
	}()
	
	ch := p.abAddChannel(brokerAddr)
	
	// Write loop
	for {
		select {
			case req := <-ch:
				err := stream.Send(req)
				if err != nil {
					fmt.Printf("Error while publishing: %v\n", err)
					p.abRemoveChannel(brokerAddr)
					break
				}
		}
	}
	
	return true
}

func (p *Publisher) abAddChannel(addr string) chan *pb.AbPubRequest {
	p.abPubChansMutex.Lock()
	defer p.abPubChansMutex.Unlock()
	
	ch := make(chan *pb.AbPubRequest, 32)
	p.abPubChans[addr] = ch
	return ch
}

func (p *Publisher) abRemoveChannel(addr string) {
	p.abPubChansMutex.Lock()
	p.abPubChansMutex.Unlock()
	
	delete(p.abPubChans, addr)
}