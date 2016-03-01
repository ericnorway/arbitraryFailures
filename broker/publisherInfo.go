package broker

import (
	"fmt"
)

// publisherInfo contains important information about a publisher
type publisherInfo struct {
	id       int64
	key      []byte
	pubCount int
}

// AddPublisher adds a Publisher to the map of Publishers.
// It takes as input the Publisher's id and shared private key.
func (b *Broker) AddPublisher(id int64, key []byte) {
	fmt.Printf("Info for publisher %v added.\n", id)

	b.publishersMutex.Lock()
	defer b.publishersMutex.Unlock()

	b.publishers[id] = publisherInfo{
		id:       id,
		key:      key,
		pubCount: 0,
	}
}

// RemovePublisher removes a Publisher from the map of Publishers.
// It takes as input the id of the Publisher.
func (b *Broker) RemovePublisher(id int64) {
	fmt.Printf("Info for publisher %v removed.\n", id)

	b.publishersMutex.Lock()
	defer b.publishersMutex.Unlock()

	delete(b.publishers, id)
}

func (b *Broker) IncrementPubCount(id int64) bool {
	b.publishersMutex.Lock()
	defer b.publishersMutex.Unlock()

	alphaReached := false
	tempPublisher := b.publishers[id]

	// Increment the publication count
	tempPublisher.pubCount++

	// Check if alpha has been reached.
	if tempPublisher.pubCount >= b.alpha {
		alphaReached = true
		tempPublisher.pubCount = 0
	}

	// Update publisher infomation
	b.publishers[id] = tempPublisher

	return alphaReached
}
