package publisher

import (
	pb "github.com/ericnorway/arbitraryFailures/proto"
)

// handleBrbPublish processes a Bracha's Reliable Broadcast publish.
// It takes as input a publication.
func (p *Publisher) handleBrbPublish(pub *pb.Publication) {
	p.brokersMutex.RLock()
	defer p.brokersMutex.RUnlock()

	for _, broker := range p.brokers {
		if broker.toCh != nil {
			select {
			case broker.toCh <- *pub:
			}
		}
	}
}
