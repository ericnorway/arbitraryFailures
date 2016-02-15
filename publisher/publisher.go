package main

import (
	"fmt"
	"io"
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
func (p *Publisher) startBrokerClient(brokerAddr string) bool {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())

	conn, err := grpc.Dial(brokerAddr, opts...)
	if err != nil {
		fmt.Printf("Error while connecting to server: %v\n", err)
		return false
	}

	client := pb.NewPubBrokerClient(conn)

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
				p.removeChannel(brokerAddr)
				break
			}
			if err != nil {
				p.removeChannel(brokerAddr)
				break
			}
		}

		fmt.Printf("Receive ended.\n")
	}()

	ch := p.addChannel(brokerAddr)

	// Write loop
	for {
		select {
		case req := <-ch:
			err := stream.Send(req)
			if err != nil {
				p.removeChannel(brokerAddr)
				break
			}
		}
	}

	return true
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
