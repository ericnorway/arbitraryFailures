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
	configFile = flag.String(
		"config",
		"",
		"The config to use for this broker.",
	)
	alpha = flag.Uint(
		"alpha",
		0,
		"The number of AB or Chain pubs to accept before requesting a BRB pub (with history).",
	)
	malevolentPercent = flag.Int(
		"mal",
		0,
		"The percentage of malicious publications to send.",
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

	if *configFile == "" {
		fmt.Printf("Need to specify a config file.\n")
		return false
	}

	return true
}
