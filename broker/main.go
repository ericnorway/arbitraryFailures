package main

func main() {
	parsedCorrectly := ParseArgs()
	if parsedCorrectly {
		StartBroker(*endpoint)
	}
}
