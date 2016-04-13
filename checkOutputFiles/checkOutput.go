package main

import (
	"flag"
)

var (
	help = flag.Bool(
		"help",
		false,
		"Show usage help",
	)
	directory = flag.String(
		"dir",
		"",
		"The directory of the files with send and receive times.",
	)
)

func usage() {
	flag.PrintDefaults()
}

func main() {
	flag.Usage = usage
	flag.Parse()
	if *help {
		flag.Usage()
		return
	}

	if *directory == "" {
		*directory = "."
	}

	r := NewResults()
	r.readLatencyFiles()
	r.calculateLatencies()

	var throughput []int
	readThroughputFiles(&throughput)
	calculateThroughputAverages(&throughput)
}
