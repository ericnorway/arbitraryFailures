package main

import (
	"fmt"
	"io/ioutil"
	"math"
	"sort"
	"strconv"
	"strings"
	//"time"
)

type Results struct {
	// Publisher:Publication:SentTime
	sendTimes map[int64]map[int64]int64
	// Publisher:Publication:Subscriber:ReceiveTime
	recvTimes map[int64]map[int64]map[int64]int64
	durations Durations
}

func NewResults() *Results {
	return &Results{
		sendTimes: make(map[int64]map[int64]int64),
		recvTimes: make(map[int64]map[int64]map[int64]int64),
	}
}

func main() {
	r := NewResults()
	
	r.readFiles()
	r.calculateDurations()
	r.calculateAverages()
}

func (r *Results) readFiles() {
	// Get all sent time file names
	files, err := ioutil.ReadDir(".")
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
		
		publisherID, err := strconv.ParseInt(lineContents[0], 10, 64)
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
		
		publisherID, err := strconv.ParseInt(lineContents[0], 10, 64)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}
		
		publicationID, err := strconv.ParseInt(lineContents[1], 10, 64)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}
		
		subscriberID, err := strconv.ParseInt(lineContents[2], 10, 64)
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
			r.recvTimes[publisherID] = make(map[int64]map[int64]int64)
		}
		if r.recvTimes[publisherID][publicationID] == nil {
			r.recvTimes[publisherID][publicationID] = make(map[int64]int64)
		}
		
		r.recvTimes[publisherID][publicationID][subscriberID] = time
	}
}

func (r *Results) calculateDurations() {
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
				// Calculate the duration
				duration := receiveTime - sendTime
				// Save the duration
				r.durations = append(r.durations, duration)
			}
		}
	}
}

type Durations []int64
func (d Durations) Len() int { return len(d) }
func (d Durations) Swap(i, j int)	 { d[i], d[j] = d[j], d[i] }
func (d Durations) Less(i, j int) bool { return d[i] < d[j] }

// calculateAverages calculates Mean, Minimum, Maximum, Median, 
// Standard deviation, and 99th percentile (optional)
func (r *Results) calculateAverages() {
	var total int64 = 0
	var length int64 = int64(len(r.durations))
	var mean int64 = 0
	var sd int64 = 0
	var ninetyninth int64 = 0
	
	if length < 1 {
		fmt.Printf("No results. Nothing to calculate.\n")
		return
	}
	
	sort.Sort(r.durations)
	
	// Calculate mean
	for i := int64(0); i < length; i++ {
		total = total + int64(r.durations[i])
	}
	mean = total / length
	
	// Calculate SD
	total = 0
	for i := int64(0); i < length; i++ {
		total = total + (r.durations[i] - mean) * (r.durations[i] - mean)
	}
	sd = int64( math.Sqrt( float64( total / length ) ) )
	
	// Calculate 99th percentile
	// position = size * percentile / 100
	i := float64(length) * 99.0 / 100.0     // position
	k := int64(i)                           // integer part of position
	f := float64(i - float64(k))            // fractional part of position
	// Calculate the value between two values in the slice
	// For example, a value between r.durations[494] and r.durations[495]
	// (1 - f) * x[k - 1] + f * x[k]
	ninetyninth = int64((1 - f) * float64(r.durations[k - 1]) + f * float64(r.durations[k]))
	
	fmt.Printf("Count:  %v\n", len(r.durations))
	fmt.Printf("Mean:   %v ns\n", mean)
	fmt.Printf("Min:    %v ns\n", r.durations[0])
	fmt.Printf("Max:    %v ns\n", r.durations[length - 1])
	fmt.Printf("Median: %v ns\n", r.durations[length / 2])
	fmt.Printf("SD:     %v ns\n", sd)
	fmt.Printf("99th:   %v ns\n", ninetyninth)
}
