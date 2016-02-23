package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/ericnorway/arbitraryFailures/common"
	pb "github.com/ericnorway/arbitraryFailures/proto"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Publisher is a struct containing a map of channels.
type Publisher struct {
	toBrokerChansMutex sync.RWMutex
	// There is one to channel for each broker.
	toBrokerChans map[int64]chan *pb.Publication
	
	brokerMutex sync.RWMutex
	brokers map[int64] BrokerInfo
}

type BrokerInfo struct {
	id int64
	key []byte
}

// NewPublisher returns a new Publisher.
func NewPublisher() *Publisher {
	return &Publisher{
		toBrokerChans: make(map[int64]chan *pb.Publication),
	}
}

// Publish publishes a publication to all the brokers.
func (p *Publisher) Publish(pubReq *pb.Publication) {
	p.toBrokerChansMutex.RLock()
	defer p.toBrokerChansMutex.RUnlock()

	for _, ch := range p.toBrokerChans {
		tempPub := &pb.Publication{}
		*tempPub = *pubReq
		tempPub.MACs = make([][]byte,1)
		tempMAC := common.CreatePublicationMAC(tempPub, []byte("12345"), common.Algorithm)
		tempPub.MACs[0] = tempMAC
		ch<- tempPub
	}
}

// StartBrokerClients starts a broker client for each broker.
// It takes as input a slice of broker addresses.
func (p *Publisher) StartBrokerClients(brokerAddrs []string) {
	for i := range brokerAddrs {
		go p.startBrokerClient(int64(i), brokerAddrs[i])
	}

	for len(p.toBrokerChans) < 3 {
		fmt.Printf("Waiting for connections...\n")
		time.Sleep(time.Second)
	}
	fmt.Printf("...done\n")
}

// startBrokerClient starts an individual broker client.
func (p *Publisher) startBrokerClient(id int64, brokerAddr string) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure(), grpc.WithBlock())

	conn, err := grpc.Dial(brokerAddr, opts...)
	if err != nil {
		fmt.Printf("Error while connecting to server: %v\n", err)
		return
	}
	defer conn.Close()

	client := pb.NewPubBrokerClient(conn)
	ch := p.addChannel(id)

	for {
		select {
		case pub := <-ch:
			resp, err := client.Publish(context.Background(), pub)
			if err != nil {
				p.removeChannel(id)
				fmt.Printf("Error publishing to %v, %v\n", id, err)
				return
			}

			if resp.AlphaReached == true {
				fmt.Printf("Alpha reached.\n")
			}
		}
	}
}

// addChannel adds a channel to the map of to broker channels.
// It returns the new channel. It takes as input the id
// of the broker.
func (p *Publisher) addChannel(id int64) chan *pb.Publication {
	fmt.Printf("Broker channel to %v added.\n", id)

	p.toBrokerChansMutex.Lock()
	defer p.toBrokerChansMutex.Unlock()

	ch := make(chan *pb.Publication, 32)
	p.toBrokerChans[id] = ch
	return ch
}

// removeChannel removes a channel from the map of to broker channels.
// It takes as input the id of the broker.
func (p *Publisher) removeChannel(id int64) {
	fmt.Printf("Broker channel to %v removed.\n", id)

	p.toBrokerChansMutex.Lock()
	p.toBrokerChansMutex.Unlock()

	delete(p.toBrokerChans, id)
}
