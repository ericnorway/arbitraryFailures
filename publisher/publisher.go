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
	brokersMutex sync.RWMutex
	brokers map[int64] BrokerInfo
	brokerConnections int64
}

// NewPublisher returns a new Publisher.
func NewPublisher() *Publisher {
	return &Publisher{
		brokers: make(map[int64]BrokerInfo),
		brokerConnections: 0,
	}
}

// Publish publishes a publication to all the brokers.
// It takes as input a publication.
func (p *Publisher) Publish(pubReq *pb.Publication) {
	p.brokersMutex.RLock()
	defer p.brokersMutex.RUnlock()

	for _, broker := range p.brokers {
		if broker.toCh != nil {
		tempPub := &pb.Publication{}
		*tempPub = *pubReq
		tempPub.MACs = make([][]byte,1)
		tempMAC := common.CreatePublicationMAC(tempPub, broker.key, common.Algorithm)
		tempPub.MACs[0] = tempMAC
		broker.toCh<- tempPub
		}
	}
}

// StartBrokerClients starts a broker client for each broker.
func (p *Publisher) StartBrokerClients() {
	for _, broker := range p.brokers {
		go p.startBrokerClient(broker)
	}

	for p.brokerConnections < 3 {
		fmt.Printf("Waiting for connections...\n")
		time.Sleep(time.Second)
	}
	fmt.Printf("...done\n")
}

// startBrokerClient starts an individual broker client.
// It takes as input the broker information.
func (p *Publisher) startBrokerClient(broker BrokerInfo) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure(), grpc.WithBlock())

	conn, err := grpc.Dial(broker.addr, opts...)
	if err != nil {
		fmt.Printf("Error while connecting to server: %v\n", err)
		return
	}
	defer conn.Close()

	client := pb.NewPubBrokerClient(conn)
	ch := p.addChannel(broker.id)

	for {
		select {
		case pub := <-ch:
			// Handle publish request and response
			resp, err := client.Publish(context.Background(), pub)
			if err != nil {
				p.removeChannel(broker.id)
				fmt.Printf("Error publishing to %v, %v\n", broker.id, err)
				return
			}

			if resp.AlphaReached == true {
				fmt.Printf("Alpha reached.\n")
			}
		}
	}
}
