package publisher

import (
	pb "github.com/ericnorway/arbitraryFailures/proto"
)

// handleBrbPublish processes a Bracha's Reliable Broadcast publish.
// It takes as input a publication. It returns a status for the publication
// as output.
func (p *Publisher) handleBrbPublish(pub *pb.Publication) pb.PubResponse_Status {
	p.brokersMutex.RLock()
	defer p.brokersMutex.RUnlock()

	for _, broker := range p.brokers {
		if broker.toCh != nil {
			select {
			case broker.toCh <- *pub:
			}
		}
	}

	statuses := make(map[pb.PubResponse_Status]uint64)

	for i := 0; i < len(p.brokers); i++ {
		select {
		case status := <-p.statusCh:
			statuses[status]++
		}
	}

	if statuses[pb.PubResponse_WAIT] > 0 {
		return pb.PubResponse_WAIT
	} else if statuses[pb.PubResponse_BAD_MAC] > 0 {
		return pb.PubResponse_BAD_MAC
	} else if statuses[pb.PubResponse_OK] >= p.quorumSize {
		return pb.PubResponse_OK
	}

	return -1
}
