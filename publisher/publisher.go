package publisher

import (
	"fmt"
	"sync"
	"time"

	"github.com/ericnorway/arbitraryFailures/common"
	pb "github.com/ericnorway/arbitraryFailures/proto"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Publisher is a struct containing a map of channels.
type Publisher struct {
	localID      uint64
	currentPubID int64

	brokersMutex      sync.RWMutex
	brokers           map[uint64]brokerInfo
	brokerConnections uint64

	historyRequestCh chan uint64
	addToHistoryCh   chan pb.Publication
}

// NewPublisher returns a new Publisher.
func NewPublisher(localID uint64) *Publisher {
	return &Publisher{
		localID:           localID,
		currentPubID:      0,
		brokers:           make(map[uint64]brokerInfo),
		brokerConnections: 0,
		historyRequestCh:  make(chan uint64, 8),
		addToHistoryCh:    make(chan pb.Publication, 8),
	}
}

// Publish publishes a publication to all the brokers.
// It takes as input a publication.
func (p *Publisher) Publish(pub *pb.Publication) {
	select {
	case p.addToHistoryCh <- *pub:
	}

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

// Start starts the publisher.
func (p *Publisher) Start() {
	go p.alphaHandler()

	for _, broker := range p.brokers {
		go p.startBrokerClient(broker)
	}

	for p.brokerConnections < 3 {
		fmt.Printf("Waiting for connections...\n")
		time.Sleep(time.Second)
	}
	fmt.Printf("...done\n")
}

// startBrokerClient starts an individual broker client.
// It takes as input the broker information.
func (p *Publisher) startBrokerClient(broker brokerInfo) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure(), grpc.WithBlock())

	conn, err := grpc.Dial(broker.addr, opts...)
	if err != nil {
		fmt.Printf("Error while connecting to server: %v\n", err)
		return
	}
	defer conn.Close()

	client := pb.NewPubBrokerClient(conn)
	ch := p.addChannel(broker.id)

	for {
		select {
		case pub := <-ch:
			pub.MACs = make([][]byte, 1)
			pub.MACs[0] = common.CreatePublicationMAC(&pub, p.brokers[broker.id].key, common.Algorithm)

			// Handle publish request and response
			resp, err := client.Publish(context.Background(), &pub)
			if err != nil {
				fmt.Printf("Error publishing to %v, %v\n", broker.id, err)
				continue
			}

			if resp.AlphaReached == true {
				select {
				case p.historyRequestCh <- broker.id:
				}
			}
		}
	}
}

// alphaHandler keeps a hisotry of the publications and sends alpha values when
// a quorum of alpha requests is reached.
func (p *Publisher) alphaHandler() {
	var history []pb.Publication
	historyRequests := make(map[uint64]bool)
	pubsSinceLastAlpha := 0
	historyID := int64(-1)

	for {
		select {
		case pub := <-p.addToHistoryCh:
			history = append(history, pub)
			pubsSinceLastAlpha++
		case id := <-p.historyRequestCh:
			historyRequests[id] = true
			if len(historyRequests) > len(p.brokers)/2 {
				// Create the publication.
				pub := &pb.Publication{
					PubType:       common.BRB,
					PublisherID:   p.localID,
					PublicationID: historyID,
					Topic:         1,
					Content:       []byte(" "),
				}

				// TODO: Add content

				// Send the publication.
				p.Publish(pub)

				historyID--

				// Reset these
				historyRequests = make(map[uint64]bool)
				pubsSinceLastAlpha = 0

				p.brokersMutex.RLock()
				for _, broker := range p.brokers {
					if broker.toCh != nil {
						select {
						case broker.toCh <- *pub:
						}
					}
				}
				p.brokersMutex.RUnlock()
			}
		}
	}
}
