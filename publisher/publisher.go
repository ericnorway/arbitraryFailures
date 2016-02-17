package main

import (
	"fmt"
	"sync"
	"time"

	pb "github.com/ericnorway/arbitraryFailures/proto"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Publisher is a struct containing a map of channels.
type Publisher struct {
	toBrokerChansMutex sync.RWMutex
	// There is one to channel for each broker.
	toBrokerChans map[string]chan *pb.Publication
}

// NewPublisher returns a new Publisher.
func NewPublisher() *Publisher {
	return &Publisher{
		toBrokerChans: make(map[string]chan *pb.Publication),
	}
}

// Publish publishes a publication to all the brokers.
func (p *Publisher) Publish(pubReq *pb.Publication) {
	p.toBrokerChansMutex.RLock()
	defer p.toBrokerChansMutex.RUnlock()

	for _, ch := range p.toBrokerChans {
		ch <- pubReq
	}
}

// StartBrokerClients starts a broker client for each broker.
// It takes as input a slice of broker addresses.
func (p *Publisher) StartBrokerClients(brokerAddrs []string) {
	for i := range brokerAddrs {
		go p.startBrokerClient(brokerAddrs[i])
	}

	for len(p.toBrokerChans) < 3 {
		fmt.Printf("Waiting for connections...\n")
		time.Sleep(time.Second)
	}
	fmt.Printf("...done\n")
}

// startBrokerClient starts an individual broker client.
func (p *Publisher) startBrokerClient(brokerAddr string) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure(), grpc.WithBlock())

	conn, err := grpc.Dial(brokerAddr, opts...)
	if err != nil {
		fmt.Printf("Error while connecting to server: %v\n", err)
		return
	}
	defer conn.Close()

	client := pb.NewPubBrokerClient(conn)
	ch := p.addChannel(brokerAddr)

	for {
		select {
		case pub := <-ch:
			resp, err := client.Publish(context.Background(), pub)
			if err != nil {
				p.removeChannel(brokerAddr)
       		fmt.Printf("Error publishing to %v, %v\n", brokerAddr, err)
       		return
			}
			
			if resp.AlphaReached == true {
				fmt.Printf("Alpha reached.\n")
			}
		}
	}
}

// addChannel adds a channel to the map of to broker channels.
// It returns the new channel. It takes as input the address
// of the broker.
func (p *Publisher) addChannel(addr string) chan *pb.Publication {
	fmt.Printf("Broker channel to %v added.\n", addr)

	p.toBrokerChansMutex.Lock()
	defer p.toBrokerChansMutex.Unlock()

	ch := make(chan *pb.Publication, 32)
	p.toBrokerChans[addr] = ch
	return ch
}

// removeChannel removes a channel from the map of to broker channels.
// It takes as input the address of the broker.
func (p *Publisher) removeChannel(addr string) {
	fmt.Printf("Broker channel to %v removed.\n", addr)

	p.toBrokerChansMutex.Lock()
	p.toBrokerChansMutex.Unlock()

	delete(p.toBrokerChans, addr)
}
