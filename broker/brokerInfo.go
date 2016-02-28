package broker

import (
	"fmt"

	pb "github.com/ericnorway/arbitraryFailures/proto"
)

// brokerInfo contains important information about a broker
type brokerInfo struct {
	id        int64
	addr      string
	key       []byte
	toEchoCh  chan pb.Publication
	toReadyCh chan pb.Publication
}

// AddBroker adds a broker to the map of brokers.
// It takes as input the broker's id, address, and shared private key.
func (b *Broker) AddBroker(id int64, addr string, key []byte) {
	fmt.Printf("Info for broker %v added.\n", id)

	b.remoteBrokersMutex.Lock()
	defer b.remoteBrokersMutex.Unlock()

	b.remoteBrokers[id] = brokerInfo{
		id:        id,
		addr:      addr,
		key:       key,
		toEchoCh:  nil,
		toReadyCh: nil,
	}
}

// RemoveBroker removes a broker from the map of brokers.
// It takes as input the id of the broker.
func (b *Broker) RemoveBroker(id int64) {
	fmt.Printf("Info for broker %v removed.\n", id)

	b.remoteBrokersMutex.Lock()
	defer b.remoteBrokersMutex.Unlock()

	delete(b.remoteBrokers, id)
}

// addBrokerChannels adds channels (echo and ready) to a broker in the broker info map.
// It returns the new channels. It takes as input the id
// of the broker.
func (b *Broker) addBrokerChannels(id int64) (chan pb.Publication, chan pb.Publication) {
	fmt.Printf("Channels to broker %v added.\n", id)

	b.remoteBrokersMutex.Lock()
	defer b.remoteBrokersMutex.Unlock()

	echoCh := make(chan pb.Publication, 32)
	readyCh := make(chan pb.Publication, 32)

	// Update channels
	tempBroker := b.remoteBrokers[id]
	tempBroker.toEchoCh = echoCh
	tempBroker.toReadyCh = readyCh
	b.remoteBrokers[id] = tempBroker

	b.remoteBrokerConnections++
	return echoCh, readyCh
}

// removeBrokerChannels removes the channels from a broker in the broker info map.
// It takes as input the id of the broker.
func (b *Broker) removeBrokerChannels(id int64) {
	fmt.Printf("Channels to broker %v removed.\n", id)

	b.remoteBrokersMutex.Lock()
	defer b.remoteBrokersMutex.Unlock()

	// Update channels
	tempBroker := b.remoteBrokers[id]
	tempBroker.toEchoCh = nil
	tempBroker.toReadyCh = nil
	b.remoteBrokers[id] = tempBroker

	b.remoteBrokerConnections--
}
