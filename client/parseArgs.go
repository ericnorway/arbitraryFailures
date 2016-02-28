package main

import (
	"flag"
	"fmt"
)

var (
	help = flag.Bool(
		"help",
		false,
		"Show usage help",
	)
	clientType = flag.String(
		"type",
		"",
		"The type of client: publisher or subscriber",
	)
	configFile = flag.String(
		"config",
		"",
		"The configuration file to use.",
	)
)

func usage() {
	flag.PrintDefaults()
}

// ParseArgs parses the command line arguments.
// The return argument indicates whether or not the function was successful.
func ParseArgs() bool {
	flag.Usage = usage
	flag.Parse()
	if *help {
		flag.Usage()
		return false
	}
	
	if *clientType != "subscriber" && *clientType != "publisher" {
		fmt.Printf("Need to specify a client type: publisher or subscriber.\n")
		return false
	}

	if *configFile == "" {
		fmt.Printf("Need to specify a config file.\n")
		return false
	}

	return true
}
