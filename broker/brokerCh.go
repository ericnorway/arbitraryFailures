package main

import (
	"fmt"
	"sync"

	pb "github.com/ericnorway/arbitraryFailures/proto"
)

type BrokerEchoChannels struct {
	sync.RWMutex
	chs map[int64]chan *pb.Publication
}

type BrokerReadyChannels struct {
	sync.RWMutex
	chs map[int64]chan *pb.Publication
}

func NewBrokerEchoChannels() *BrokerEchoChannels {
	return &BrokerEchoChannels{
		chs: make(map[int64]chan *pb.Publication),
	}
}

func NewBrokerReadyChannels() *BrokerReadyChannels {
	return &BrokerReadyChannels{
		chs: make(map[int64]chan *pb.Publication),
	}
}

func (b *BrokerEchoChannels) AddBrokerEchoChannel(id int64) chan *pb.Publication {
	fmt.Printf("Broker echo channel %v added.\n", id)
	ch := make(chan *pb.Publication, 32)

	b.Lock()
	defer b.Unlock()

	b.chs[id] = ch

	return ch
}

func (b *BrokerEchoChannels) RemoveBrokerEchoChannel(id int64) {
	fmt.Printf("Broker echo channel %v removed.\n", id)

	b.Lock()
	b.Unlock()

	delete(b.chs, id)
}

func (b *BrokerReadyChannels) AddBrokerReadyChannel(id int64) chan *pb.Publication {
	fmt.Printf("Broker ready channel %v added.\n", id)
	ch := make(chan *pb.Publication, 32)

	b.Lock()
	defer b.Unlock()

	b.chs[id] = ch

	return ch
}

func (b *BrokerReadyChannels) RemoveBrokerReadyChannel(id int64) {
	fmt.Printf("Broker ready channel %v removed.\n", id)

	b.Lock()
	b.Unlock()

	delete(b.chs, id)
}
