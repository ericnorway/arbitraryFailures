package publisher

import (
	pb "github.com/ericnorway/arbitraryFailures/proto"
)

// handleBrbPublish processes a Bracha's Reliable Broadcast publish.
// It takes as input a publication.
func (p *Publisher) handleBrbPublish(pub *pb.Publication) bool {
	p.brokersMutex.RLock()
	defer p.brokersMutex.RUnlock()

	acceptCount := uint64(0)

	for _, broker := range p.brokers {
		if broker.toCh != nil {
			select {
			case broker.toCh <- *pub:
			}
		}
	}

	for i := 0; i < len(p.brokers); i++ {
		select {
		case accepted := <-p.acceptedCh:
			if accepted {
				acceptCount++
			}
		}
	}

	if acceptCount >= p.quorumSize {
		return true
	}

	return false
}
