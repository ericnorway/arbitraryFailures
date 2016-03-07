package broker

func (b Broker) checkDoubleAlphaCounter(publisherID uint64, topicID uint64) bool {
	if b.alphaCounters[publisherID] == nil {
		b.alphaCounters[publisherID] = make(map[uint64]uint64)
	}

	if b.alphaCounters[publisherID][topicID] >= 2*b.alpha {
		return true
	}
	return false
}

func (b Broker) incrementAlphaCounter(publisherID uint64, topicID uint64) bool {
	if b.alphaCounters[publisherID] == nil {
		b.alphaCounters[publisherID] = make(map[uint64]uint64)
	}

	b.alphaCounters[publisherID][topicID]++

	if b.alphaCounters[publisherID][topicID] == b.alpha {
		return true
	}

	return false
}

func (b Broker) resetAlphaCounter(publisherID uint64, topicID uint64) {
	if b.alphaCounters[publisherID] == nil {
		b.alphaCounters[publisherID] = make(map[uint64]uint64)
	}

	b.alphaCounters[publisherID][topicID] = 0
}
