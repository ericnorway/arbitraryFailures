package main

import (
	"fmt"
	"sync"

	pb "github.com/ericnorway/arbitraryFailures/proto"
)

type ToBrokerEchoChannels struct {
	sync.RWMutex
	chs map[int64]chan *pb.Publication
}

type ToBrokerReadyChannels struct {
	sync.RWMutex
	chs map[int64]chan *pb.Publication
}

func NewToBrokerEchoChannels() *ToBrokerEchoChannels {
	return &ToBrokerEchoChannels{
		chs: make(map[int64]chan *pb.Publication),
	}
}

func NewToBrokerReadyChannels() *ToBrokerReadyChannels {
	return &ToBrokerReadyChannels{
		chs: make(map[int64]chan *pb.Publication),
	}
}

func (b *ToBrokerEchoChannels) AddToBrokerEchoChannel(id int64) chan *pb.Publication {
	fmt.Printf("To broker echo channel %v added.\n", id)
	ch := make(chan *pb.Publication, 32)

	b.Lock()
	defer b.Unlock()

	b.chs[id] = ch

	return ch
}

func (b *ToBrokerEchoChannels) RemoveToBrokerEchoChannel(id int64) {
	fmt.Printf("To broker echo channel %v removed.\n", id)

	b.Lock()
	b.Unlock()

	delete(b.chs, id)
}

func (b *ToBrokerReadyChannels) AddToBrokerReadyChannel(id int64) chan *pb.Publication {
	fmt.Printf("To broker ready channel %v added.\n", id)
	ch := make(chan *pb.Publication, 32)

	b.Lock()
	defer b.Unlock()

	b.chs[id] = ch

	return ch
}

func (b *ToBrokerReadyChannels) RemoveToBrokerReadyChannel(id int64) {
	fmt.Printf("To broker ready channel %v removed.\n", id)

	b.Lock()
	b.Unlock()

	delete(b.chs, id)
}
