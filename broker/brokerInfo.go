package main

import (
	// "fmt"
	"sync"

	pb "github.com/ericnorway/arbitraryFailures/proto"
)

// ToBrokerEchoChannels is a struct containing channels for sending echoes.
type ToBrokerEchoChannels struct {
	sync.RWMutex
	chs map[int64]chan *pb.Publication
}

// ToBrokerReadyChannels is a struct containing channels for sending readies.
type ToBrokerReadyChannels struct {
	sync.RWMutex
	chs map[int64]chan *pb.Publication
}

// NewToBrokerEchoChannels returns a new ToBrokerEchoChannels.
func NewToBrokerEchoChannels() *ToBrokerEchoChannels {
	return &ToBrokerEchoChannels{
		chs: make(map[int64]chan *pb.Publication),
	}
}

// NewToBrokerReadyChannels returns a new ToBrokerReadyChannels.
func NewToBrokerReadyChannels() *ToBrokerReadyChannels {
	return &ToBrokerReadyChannels{
		chs: make(map[int64]chan *pb.Publication),
	}
}

// AddToBrokerEchoChannel adds a channel to ToBrokerEchoChannels.
// It also returns the channel. It takes as input the ID of the broker that will
// receive echoes.
func (b *ToBrokerEchoChannels) AddToBrokerEchoChannel(id int64) chan *pb.Publication {
	// fmt.Printf("To broker echo channel %v added.\n", id)
	ch := make(chan *pb.Publication, 32)

	b.Lock()
	defer b.Unlock()

	b.chs[id] = ch

	return ch
}

// RemoveToBrokerEchoChannel removes a channel from ToBrokerEchoChannels.
// It takes as input the ID of the broker that will no longer receive echoes.
func (b *ToBrokerEchoChannels) RemoveToBrokerEchoChannel(id int64) {
	// fmt.Printf("To broker echo channel %v removed.\n", id)

	b.Lock()
	b.Unlock()

	delete(b.chs, id)
}

// AddToBrokerReadyChannel adds a channel to ToBrokerReadyChannels.
// It also returns the channel. It takes as input the ID of the broker that will
// receive readies.
func (b *ToBrokerReadyChannels) AddToBrokerReadyChannel(id int64) chan *pb.Publication {
	// fmt.Printf("To broker ready channel %v added.\n", id)
	ch := make(chan *pb.Publication, 32)

	b.Lock()
	defer b.Unlock()

	b.chs[id] = ch

	return ch
}

// RemoveToBrokerReadyChannel removes a channel from ToBrokerReadyChannels.
// It takes as input the ID of the broker that will no longer receive readies.
func (b *ToBrokerReadyChannels) RemoveToBrokerReadyChannel(id int64) {
	// fmt.Printf("To broker ready channel %v removed.\n", id)

	b.Lock()
	b.Unlock()

	delete(b.chs, id)
}
