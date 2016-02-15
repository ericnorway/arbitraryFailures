package main

// main parses the command line arguments and starts the Broker
func main() {
	parsedCorrectly := ParseArgs()
	if parsedCorrectly {
		StartBroker(*endpoint)
	}
}
