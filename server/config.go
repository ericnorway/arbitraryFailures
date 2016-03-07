package main

import (
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
)

var tagID = "ID"
var tagPublisherKeys = "PUB_KEYS"
var tagSubscriberKeys = "SUB_KEYS"
var tagBrokerKeys = "BRK_KEYS"
var tagBrokerAddrs = "BRK_ADDR"
var tagChain = "CHAIN"
var localID uint64
var publisherKeys []string
var subscriberKeys []string
var brokerKeys []string
var brokerAddresses []string
var chain [][]string

// ReadConfigFile reads a config file and updated the values of global variables.
// It returns an error, if any.
func ReadConfigFile(fileName string) error {
	// Read the whole file
	bytes, err := ioutil.ReadFile(fileName)
	if err != nil {
		return err
	}

	// Break the file up into lines
	lines := strings.Split(string(bytes), "\n")

	for i := range lines {
		// Break each line into a key and value
		lineContents := strings.Split(lines[i], "=")

		// Get the values for each key
		switch {
		case lineContents[0] == tagID:
			if len(lineContents) == 2 {
				localID, err = strconv.ParseUint(lineContents[1], 10, 64)
				if err != nil {
					return err
				}
			} else {
				return fmt.Errorf("Error parsing the config file entry for %s.\n", lineContents[0])
			}
		case lineContents[0] == tagPublisherKeys:
			if len(lineContents) == 2 {
				publisherKeys = strings.Split(lineContents[1], ",")
			} else {
				return fmt.Errorf("Error parsing the config file entry for %s.\n", lineContents[0])
			}
		case lineContents[0] == tagSubscriberKeys:
			if len(lineContents) == 2 {
				subscriberKeys = strings.Split(lineContents[1], ",")
			} else {
				return fmt.Errorf("Error parsing the config file entry for %s.\n", lineContents[0])
			}
		case lineContents[0] == tagBrokerKeys:
			if len(lineContents) == 2 {
				brokerKeys = strings.Split(lineContents[1], ",")
			} else {
				return fmt.Errorf("Error parsing the config file entry for %s.\n", lineContents[0])
			}
		case lineContents[0] == tagBrokerAddrs:
			if len(lineContents) == 2 {
				brokerAddresses = strings.Split(lineContents[1], ",")
			} else {
				return fmt.Errorf("Error parsing the config file entry for %s.\n", lineContents[0])
			}
		case lineContents[0] == tagChain:
			if len(lineContents) == 2 {
				temp := strings.Split(lineContents[1], ",")
				for i := range temp {
					chain = append(chain, strings.Split(temp[i], ":"))
				}
			} else {
				return fmt.Errorf("Error parsing the config file entry for %s.\n", lineContents[0])
			}
		default:
		}
	}

	if len(brokerAddresses) != len(brokerKeys) {
		return fmt.Errorf("The number of broker addresses is not the same as the number of broker keys.")
	}

	return nil
}
