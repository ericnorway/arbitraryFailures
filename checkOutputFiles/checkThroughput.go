package main

import (
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

	var throughput []int
	readFiles(&throughput)
	calculateAverages(&throughput)
}

func readFiles(throughput *[]int) {
	// Get all sent time file names
	files, err := ioutil.ReadDir(*directory)
	if err != nil {
		fmt.Printf("%v\n", err)
	}

	for _, file := range files {
		fileName := file.Name()
		if strings.HasSuffix(fileName, ".txt") {
			if strings.HasPrefix(fileName, "throughput") {
				readThroughputFiles(fileName, throughput)
			}
		}
	}
}

func readThroughputFiles(fileName string, throughput *[]int) {
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

// calculateAverages calculates Mean, Minimum, Maximum, Median,
// Standard deviation, and 99th percentile (optional)
func calculateAverages(throughput *[]int) {
	var total float64 = 0
	var length int = len(*throughput)
	var fLength float64 = float64(len(*throughput))
	var mean float64 = 0
	var sd float64 = 0
	//var ninetyninth float64 = 0

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

	// Calculate 99th percentile
	// position = size * percentile / 100
	//i := fLength * 99.0 / 100.0  // position
	//k := int(i)                  // integer part of position
	//f := float64(i - float64(k)) // fractional part of position
	// Calculate the value between two values in the slice
	// For example, a value between r.durations[494] and r.durations[495]
	// (1 - f) * x[k - 1] + f * x[k]
	//ninetyninth = (1-f)*float64((*throughput)[k-1]) + f*float64((*throughput)[k])

	//fmt.Printf("Count\t%v\n", len(*throughput))
	fmt.Printf("Mean\t%v\tPPS\n", mean)
	fmt.Printf("Min\t%v\tPPS\n", (*throughput)[0])
	fmt.Printf("Max\t%v\tPPS\n", (*throughput)[length-1])
	fmt.Printf("Median\t%v\tPPS\n", (*throughput)[length/2])
	fmt.Printf("SD\t%v\tnPPS\n", sd)
	//fmt.Printf("99th\t%v\tns\n", ninetyninth)
}
