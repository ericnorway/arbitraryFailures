package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	//"time"
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

type Results struct {
	// Publisher:Publication:SentTime
	sendTimes map[uint64]map[int64]int64
	// Publisher:Publication:Subscriber:ReceiveTime
	recvTimes map[uint64]map[int64]map[uint64]int64
	latencies Latencies
}

func NewResults() *Results {
	return &Results{
		sendTimes: make(map[uint64]map[int64]int64),
		recvTimes: make(map[uint64]map[int64]map[uint64]int64),
	}
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

	r.readFiles()
	r.calculateLatencies()
	r.calculateAverages()
}

func (r *Results) readFiles() {
	// Get all sent time file names
	files, err := ioutil.ReadDir(*directory)
	if err != nil {
		fmt.Printf("%v\n", err)
	}

	for _, file := range files {
		fileName := file.Name()
		if strings.HasSuffix(fileName, ".txt") {
			if strings.HasPrefix(fileName, "sendTimes") {
				r.readSentTimeFile(fileName)
			} else if strings.HasPrefix(fileName, "recvTimes") {
				r.readReceivedTimeFile(fileName)
			}
		}
	}
}

func (r *Results) readSentTimeFile(fileName string) {
	fileName = filepath.Join(*directory, fileName)

	// Read the whole file
	bytes, err := ioutil.ReadFile(fileName)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}

	// Break the file up into lines
	lines := strings.Split(string(bytes), "\n")

	for i := range lines {
		if lines[i] == "" {
			continue
		}

		// Split each line
		lineContents := strings.Split(lines[i], ":")
		if len(lineContents) != 3 {
			fmt.Printf("Error parsing: %v\n", lines[i])
			continue
		}

		publisherID, err := strconv.ParseUint(lineContents[0], 10, 64)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}

		publicationID, err := strconv.ParseInt(lineContents[1], 10, 64)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}

		time, err := strconv.ParseInt(lineContents[2], 10, 64)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}

		if r.sendTimes[publisherID] == nil {
			r.sendTimes[publisherID] = make(map[int64]int64)
		}

		r.sendTimes[publisherID][publicationID] = time
	}
}

func (r *Results) readReceivedTimeFile(fileName string) {
	fileName = filepath.Join(*directory, fileName)

	// Read the whole file
	bytes, err := ioutil.ReadFile(fileName)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}

	// Break the file up into lines
	lines := strings.Split(string(bytes), "\n")

	for i := range lines {
		if lines[i] == "" {
			continue
		}

		// Split each line
		lineContents := strings.Split(lines[i], ":")
		if len(lineContents) != 4 {
			fmt.Printf("Error parsing: %v\n", lines[i])
			continue
		}

		publisherID, err := strconv.ParseUint(lineContents[0], 10, 64)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}

		publicationID, err := strconv.ParseInt(lineContents[1], 10, 64)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}

		subscriberID, err := strconv.ParseUint(lineContents[2], 10, 64)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}

		time, err := strconv.ParseInt(lineContents[3], 10, 64)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}

		if r.recvTimes[publisherID] == nil {
			r.recvTimes[publisherID] = make(map[int64]map[uint64]int64)
		}
		if r.recvTimes[publisherID][publicationID] == nil {
			r.recvTimes[publisherID][publicationID] = make(map[uint64]int64)
		}

		r.recvTimes[publisherID][publicationID][subscriberID] = time
	}
}

func (r *Results) calculateLatencies() {
	// For each publisher
	for publisher := range r.sendTimes {
		if r.recvTimes[publisher] == nil {
			fmt.Printf("No subscriber received any publication from publisher %v.\n", publisher)
			continue
		}

		// For each publication
		for publication, sendTime := range r.sendTimes[publisher] {
			if r.recvTimes[publisher][publication] == nil {
				fmt.Printf("No subscriber received publication %v from publisher %v.\n", publication, publisher)
				continue
			}

			// For each subscriber
			for _, receiveTime := range r.recvTimes[publisher][publication] {
				// Calculate the latency. Divide by 1000 to convert to microseconds
				latency := (receiveTime - sendTime) / 1000
				// Save the latency
				r.latencies = append(r.latencies, latency)
			}
		}
	}
}

type Latencies []int64

func (l Latencies) Len() int           { return len(l) }
func (l Latencies) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }
func (l Latencies) Less(i, j int) bool { return l[i] < l[j] }

// calculateAverages calculates Mean, Minimum, Maximum, Median,
// Standard deviation, and 99th percentile (optional)
func (r *Results) calculateAverages() {
	var total int64 = 0
	var length int64 = int64(len(r.latencies))
	var mean int64 = 0
	var sd int64 = 0
	var ninetyninth int64 = 0

	if length < 1 {
		fmt.Printf("No results. Nothing to calculate.\n")
		return
	}

	outputFileName := filepath.Join(*directory, "output.txt")
	var buf bytes.Buffer
	for i := int64(0); i < length; i++ {
		//fmt.Printf("%v\n", float64(r.latencies[i]) / 1000.0)
		buf.WriteString(strconv.FormatFloat(float64(r.latencies[i])/1000.0, 'f', 6, 64))
		buf.WriteString("\n")
	}
	ioutil.WriteFile(outputFileName, []byte(buf.String()), 0644)

	// Sort for getting min, max, median, 99th
	sort.Sort(r.latencies)

	// Calculate mean
	for i := int64(0); i < length; i++ {
		total = total + int64(r.latencies[i])
	}
	mean = total / length

	// Calculate SD
	total = 0
	for i := int64(0); i < length; i++ {
		total = total + (r.latencies[i]-mean)*(r.latencies[i]-mean)
	}
	sd = int64(math.Sqrt(float64(total / length)))

	// Calculate 99th percentile
	// position = size * percentile / 100
	i := float64(length) * 99.0 / 100.0 // position
	k := int64(i)                       // integer part of position
	f := float64(i - float64(k))        // fractional part of position
	// Calculate the value between two values in the slice
	// For example, a value between r.latencies[494] and r.latencies[495]
	// (1 - f) * x[k - 1] + f * x[k]
	ninetyninth = int64((1-f)*float64(r.latencies[k-1]) + f*float64(r.latencies[k]))

	fmt.Printf("Count\t%v\n", len(r.latencies))
	fmt.Printf("Mean\t%v\tms\n", float64(mean)/1000.0)
	fmt.Printf("Min\t%v\tms\n", float64(r.latencies[0])/1000.0)
	fmt.Printf("Max\t%v\tms\n", float64(r.latencies[length-1])/1000.0)
	fmt.Printf("Median\t%v\tms\n", float64(r.latencies[length/2])/1000.0)
	fmt.Printf("SD\t%v\tms\n", float64(sd)/1000.0)
	fmt.Printf("99th\t%v\tms\n", float64(ninetyninth)/1000.0)
}
