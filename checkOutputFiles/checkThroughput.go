package main

import (
	"fmt"
	"io/ioutil"
	"math"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

func readThroughputFiles(throughput *[]int) {
	// Get all sent time file names
	files, err := ioutil.ReadDir(*directory)
	if err != nil {
		fmt.Printf("%v\n", err)
	}

	for _, file := range files {
		fileName := file.Name()
		if strings.HasSuffix(fileName, ".txt") {
			if strings.HasPrefix(fileName, "throughput") {
				readThroughputFile(fileName, throughput)
			}
		}
	}
}

func readThroughputFile(fileName string, throughput *[]int) {
	fileName = filepath.Join(*directory, fileName)

	// Read the whole file
	bytes, err := ioutil.ReadFile(fileName)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}

	// Break the file up into lines
	lines := strings.Split(string(bytes), "\n")

	for _, line := range lines {
		if line == "" {
			continue
		}

		value, err := strconv.Atoi(line)

		if err != nil {
			fmt.Printf("%v\n", err)
			continue
		}

		*throughput = append(*throughput, value)
	}
}

// calculateThroughputAverages calculates Mean, Minimum, Maximum, Median,
// Standard deviation, and 99th percentile (optional)
func calculateThroughputAverages(throughput *[]int) {
	var total float64 = 0
	var length int = len(*throughput)
	var fLength float64 = float64(len(*throughput))
	var mean float64 = 0
	var sd float64 = 0

	if length < 1 {
		fmt.Printf("No results. Nothing to calculate.\n")
		return
	}

	// Sort for getting min, max, median, 99th
	sort.Ints(*throughput)

	// Calculate mean
	for i := 0; i < length; i++ {
		total = total + float64((*throughput)[i])
	}
	mean = total / fLength

	// Calculate SD
	total = 0
	for i := 0; i < length; i++ {
		total = total + (float64((*throughput)[i])-mean)*(float64((*throughput)[i])-mean)
	}
	sd = math.Sqrt(total / fLength)

	fmt.Printf("*********************\n")
	fmt.Printf("THROUGHPUT:\n")
	fmt.Printf("Mean\t%.2f\tPPS\n", mean)
	fmt.Printf("Min\t%v\tPPS\n", (*throughput)[0])
	fmt.Printf("Max\t%v\tPPS\n", (*throughput)[length-1])
	fmt.Printf("Median\t%v\tPPS\n", (*throughput)[length/2])
	fmt.Printf("SD\t%.2f\tPPS\n", sd)
	fmt.Printf("*********************\n")
}
