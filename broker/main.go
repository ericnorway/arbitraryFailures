package main

// main parses the command line arguments and starts the Broker
func main() {
	parsedCorrectly := ParseArgs()
	if !parsedCorrectly {
		return
	}

	broker := NewBroker(*endpoint)

	broker.AddPublisher(1, []byte("12345"))
	broker.AddPublisher(2, []byte("12345"))
	broker.AddPublisher(3, []byte("12345"))
	broker.AddPublisher(4, []byte("12345"))

	if *endpoint != "localhost:11111" {
		broker.AddBroker(1, "localhost:11111", []byte("12345"))
	}
	if *endpoint != "localhost:11112" {
		broker.AddBroker(2, "localhost:11112", []byte("12345"))
	}
	if *endpoint != "localhost:11113" {
		broker.AddBroker(3, "localhost:11113", []byte("12345"))
	}
	if *endpoint != "localhost:11114" {
		broker.AddBroker(4, "localhost:11114", []byte("12345"))
	}

	broker.AddSubscriber(1, []byte("12345"))
	broker.AddSubscriber(2, []byte("12345"))
	broker.AddSubscriber(3, []byte("12345"))
	broker.AddSubscriber(4, []byte("12345"))

	broker.StartBroker()
}
