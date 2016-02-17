package main

import (
	"fmt"
	"sync"

	pb "github.com/ericnorway/arbitraryFailures/proto"
)

// Brokers is a struct containing a map of brokers' information.
type Brokers struct {
	sync.RWMutex
	fromBrokerCh chan *pb.Publication
	brokers map[int64]BrokerInfo
}

// NewBrokers returns a new Brokers.
func NewBrokers() *Brokers {
	return &Brokers{
		fromBrokerCh: make(chan *pb.Publication, 32),
		brokers: make(map[int64]BrokerInfo),
	}
}

// BrokerInfo is a struct containing information about the broker.
type BrokerInfo struct {
	id      int64
	addr    string
	toBrokerCh chan *pb.Publication
}

// AddBrokerInfo adds a new BrokerInfo to Brokers. It returns the new BrokerInfo.
// It takes as input the Broker ID and address.
func (p *Brokers) AddBrokerInfo(id int64, addr string) BrokerInfo {
	fmt.Printf("Broker information for broker %v added.\n", id)

	brokerInfo := BrokerInfo{
		id:      id,
		addr:    addr,
		toBrokerCh: make(chan *pb.Publication, 32),
	}

	p.Lock()
	defer p.Unlock()

	p.brokers[id] = brokerInfo

	return brokerInfo
}

// RemoveBrokerInfo removes a BrokerInfo from Brokers.
// It takes as input the Broker ID
func (p *Brokers) RemoveBrokerInfo(id int64) {
	fmt.Printf("Broker information for broker %v removed.\n", id)

	p.Lock()
	p.Unlock()

	delete(p.brokers, id)
}
