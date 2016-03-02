package main

import (
	"flag"
	"fmt"

	"github.com/ericnorway/arbitraryFailures/common"
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
	pubCount = flag.Int64(
		"pubCount",
		10,
		"The number of publications to send (publisher only).",
	)
	pubType = flag.String(
		"pubType",
		"AB",
		"The algorithm to use: AB, BRB, Chain (publisher only).",
	)
	publicationType  uint32
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

	if *clientType == "publisher" {

		switch *pubType {
		case "AB":
			publicationType = common.AB
		case "BRB":
			publicationType = common.BRB
		case "Chain":
			publicationType = common.Chain
		default:
			publicationType = common.AB
		}
	}

	return true
}
