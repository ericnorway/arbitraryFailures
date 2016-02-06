These are just temporary files I'm using while learning gorums.

Start the subscribers first.

`go run ./subscriber/subscriber.go -endpoint="localhost:11115"`

`go run ./subscriber/subscriber.go -endpoint="localhost:11114"`

Then the brokers.

`go run ./broker/broker.go -endpoint="localhost:11113" -id=3`

`go run ./broker/broker.go -endpoint="localhost:11112" -id=2`

`go run ./broker/broker.go -endpoint="localhost:11111" -id=1`

Finally the publisher.

`go run ./publisher/publisher.go`
