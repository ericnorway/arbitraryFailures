package broker

import (
//"fmt"
)

// publisherInfo contains important information about a publisher
type publisherInfo struct {
	id  uint64
	key []byte
}

// AddPublisher adds a Publisher to the map of Publishers.
// It takes as input the Publisher's id and shared private key.
func (b *Broker) AddPublisher(id uint64, key []byte) {
	//fmt.Printf("Info for publisher %v added.\n", id)

	b.publishersMutex.Lock()
	defer b.publishersMutex.Unlock()

	b.publishers[id] = publisherInfo{
		id:  id,
		key: key,
	}
}

// RemovePublisher removes a Publisher from the map of Publishers.
// It takes as input the id of the Publisher.
func (b *Broker) RemovePublisher(id uint64) {
	//fmt.Printf("Info for publisher %v removed.\n", id)

	b.publishersMutex.Lock()
	defer b.publishersMutex.Unlock()

	delete(b.publishers, id)
}
