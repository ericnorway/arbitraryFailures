package broker

// checkDoubleAlphaCounter checks to see if the broker should block this
// publisher/topic or not. It returns whether or not to block.
// It takes as input the publisher ID and the topic ID.
func (b Broker) checkDoubleAlphaCounter(publisherID uint64, topicID uint64) bool {
	if b.alphaCounters[publisherID] == nil {
		b.alphaCounters[publisherID] = make(map[uint64]uint64)
	}

	if b.alphaCounters[publisherID][topicID] >= 2*b.alpha {
		return true
	}
	return false
}

// incrementAlphaCounter increments the counter for this publisher/topic
// and it checks to see if the broker should request a history publication.
// It returns whether or not to request a history publication.
// It takes as input the publisher ID and the topic ID.
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

// resetAlphaCounter resets the counter for this publisher/topic.
// It takes as input the publisher ID and the topic ID.
func (b Broker) resetAlphaCounter(publisherID uint64, topicID uint64) {
	if b.alphaCounters[publisherID] == nil {
		b.alphaCounters[publisherID] = make(map[uint64]uint64)
	}

	b.alphaCounters[publisherID][topicID] = 0
}
