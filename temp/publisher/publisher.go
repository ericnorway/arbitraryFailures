package main

import (
	"fmt"
	"time"

	pb "github.com/ericnorway/arbitraryFailures/temp/proto"

	"google.golang.org/grpc"
)

func main() {
	fmt.Printf("Publisher started.\n")

	grpcOpts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithTimeout(50 * time.Millisecond),
		grpc.WithInsecure(),
	}
	dialOpts := pb.WithGrpcDialOptions(grpcOpts...)

	mgr, err := pb.NewManager(
		[]string{"localhost:11111", "localhost:11112", "localhost:11113"},
		dialOpts,
	)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}
	defer mgr.Close()

	// Get all all available machine ids
	ids := mgr.MachineIDs()

	// A publish configuration
	pubConfig, err := mgr.NewConfiguration(ids, 3, time.Second)
	if err != nil {
		fmt.Printf("error creating read config: %v\n", err)
		return
	}
	
	pubConfig.Publish(&pb.PubRequest{
		PublisherID: 1,
		PublicationID: 1,
		Publication: []byte(time.Now().String()),
	})
	time.Sleep(2 * time.Second)
	
	pubConfig.Publish(&pb.PubRequest{
		PublisherID: 1,
		PublicationID: 2,
		Publication: []byte(time.Now().String()),
	})
	time.Sleep(2 * time.Second)
	
	pubConfig.Publish(&pb.PubRequest{
		PublisherID: 1,
		PublicationID: 3,
		Publication: []byte(time.Now().String()),
	})
	time.Sleep(2 * time.Second) // Make sure publisher stays alive long enough to send all messages.
}
