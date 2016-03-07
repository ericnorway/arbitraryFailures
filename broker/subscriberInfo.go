package broker

import (
	//"fmt"

	pb "github.com/ericnorway/arbitraryFailures/proto"
)

// subscriberInfo contains important information about a subscriber
type subscriberInfo struct {
	id     uint64
	key    []byte
	toCh   chan pb.Publication
	topics map[uint64]bool
}

// AddSubscriber adds a subscriber to the map of subscribers.
// It takes as input the subscriber's id and shared private key.
func (b *Broker) AddSubscriber(id uint64, key []byte) {
	//fmt.Printf("Info for subscriber %v added.\n", id)

	b.subscribersMutex.Lock()
	defer b.subscribersMutex.Unlock()

	b.subscribers[id] = subscriberInfo{
		id:     id,
		key:    key,
		toCh:   nil,
		topics: make(map[uint64]bool),
	}
}

// RemoveSubscriber removes a subscriber from the map of subscribers.
// It takes as input the id of the subscriber.
func (b *Broker) RemoveSubscriber(id uint64) {
	//fmt.Printf("Info for subscriber %v removed.\n", id)

	b.subscribersMutex.Lock()
	defer b.subscribersMutex.Unlock()

	delete(b.subscribers, id)
}

// addToSubChannel adds a channel to a subscriber in the subscriber info map.
// It returns the new channel. It takes as input the id
// of the subscriber.
func (b *Broker) addToSubChannel(id uint64) chan pb.Publication {
	//fmt.Printf("Channel to subscriber %v added.\n", id)

	b.subscribersMutex.Lock()
	defer b.subscribersMutex.Unlock()

	ch := make(chan pb.Publication, 32)

	// Update channel
	tempSubscriber := b.subscribers[id]
	tempSubscriber.toCh = ch
	b.subscribers[id] = tempSubscriber

	return ch
}

// removeToSubChannel removes a channel from a subscriber in the subscriber info map.
// It takes as input the id of the subscriber.
func (b *Broker) removeToSubChannel(id uint64) {
	//fmt.Printf("Channel to subscriber %v removed.\n", id)

	b.subscribersMutex.Lock()
	defer b.subscribersMutex.Unlock()

	// Update channel
	tempSubscriber := b.subscribers[id]
	tempSubscriber.toCh = nil
	b.subscribers[id] = tempSubscriber
}

// changeTopics updates a subscriber's topics.
// It takes as input the subscription request.
func (b Broker) changeTopics(req *pb.SubRequest) {

	for i := range b.subscribers[req.SubscriberID].topics {
		b.subscribers[req.SubscriberID].topics[i] = false
	}

	for _, topic := range req.TopicIDs {
		b.subscribers[req.SubscriberID].topics[topic] = true
	}
}
