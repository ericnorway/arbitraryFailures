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
	publisherID = flag.Int(
		"id",
		0,
		"The ID for this publisher.",
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

	if *publisherID == 0 {
		fmt.Printf("Need to specify an ID.\n")
		return false
	}

	return true
}
