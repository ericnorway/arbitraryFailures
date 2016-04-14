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

	tempThroughput := []int{}

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

		tempThroughput = append(tempThroughput, value)
	}

	// Remove leading zeroes (from before the publishers started)
	removeLeadingZeroes(&tempThroughput)

	// Remove trailing zeroes (from after the publishers finished)
	removeTrailingZeroes(&tempThroughput)

	*throughput = append(*throughput, tempThroughput[:]...)
}

// removeTrailingZeroes removes the leading zeroes in the slice.
// It takes as input a pointer to the slice.
func removeLeadingZeroes(throughput *[]int) {
	// Just return for empty slice
	if len(*throughput) < 1 {
		return
	}

	// While the first element in the slice is zero
	for i := 0; (*throughput)[i] == 0; {
		// If final element in slice is zero
		if len(*throughput) == 1 {
			*throughput = []int{}
			break
		} else {
			// Remove the first element
			*throughput = append((*throughput)[:i], (*throughput)[i+1:]...)
		}
	}
}

// removeTrailingZeroes removes the trailing zeroes in the slice.
// It takes as input a pointer to the slice.
func removeTrailingZeroes(throughput *[]int) {
	// Just return for empty slice
	if len(*throughput) < 1 {
		return
	}

	// While the last element in the slice is zero
	for i := len(*throughput) - 1; (*throughput)[i] == 0; i-- {
		// If the final element in the slice is zero
		if len(*throughput) == 1 {
			*throughput = []int{}
			break
		} else {
			// Remove the last element
			*throughput = append((*throughput)[:i])
		}
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
