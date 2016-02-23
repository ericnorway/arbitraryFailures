package main

import (
	"fmt"

	pb "github.com/ericnorway/arbitraryFailures/proto"
)

type BrokerInfo struct {
	id int64
	addr string
	key []byte
	toCh chan *pb.Publication
}

// AddBroker adds a broker to the map of brokers.
// It takes as input the broker's id, address, and shared private key.
func (s *Subscriber) AddBroker(id int64, addr string, key []byte) {
	fmt.Printf("Broker info to %v added.\n", id)

	s.brokersMutex.Lock()
	defer s.brokersMutex.Unlock()

	s.brokers[id] = BrokerInfo{
		id: id,
		addr: addr,
		key: key,
		toCh: nil,
	}
}

// RemoveBroker removes a broker from the map of brokers.
// It takes as input the id of the broker.
func (s *Subscriber) RemoveBroker(id int64) {
	fmt.Printf("Broker info to %v removed.\n", id)

	s.brokersMutex.Lock()
	defer s.brokersMutex.Unlock()
	
	delete(s.brokers, id)
}

// addChannel adds a channel to a broker in the broker info map.
// It returns the new channel. It takes as input the id
// of the broker.
func (s *Subscriber) addChannel(id int64) chan *pb.Publication {
	fmt.Printf("Broker channel to %v added.\n", id)

	s.brokersMutex.Lock()
	defer s.brokersMutex.Unlock()

	ch := make(chan *pb.Publication, 32)
	
	// Update channel
	tempBroker := s.brokers[id]
	tempBroker.toCh = ch
	s.brokers[id] = tempBroker
	
	s.brokerConnections++
	return ch
}

// removeChannel removes a channel from a broker in the broker info map.
// It takes as input the id of the broker.
func (s *Subscriber) removeChannel(id int64) {
	fmt.Printf("Broker channel to %v removed.\n", id)

	s.brokersMutex.Lock()
	defer s.brokersMutex.Unlock()
	
	// Update channel
	tempBroker := s.brokers[id]
	tempBroker.toCh = nil
	s.brokers[id] = tempBroker
	
	s.brokerConnections--
}
