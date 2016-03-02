package main

import (
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
)

var tagID = "ID"
var tagBrokerKeys = "BRK_KEYS"
var tagBrokerAddrs = "BRK_ADDR"
var tagTopics = "TOPICS"
var localID uint64
var brokerKeys []string
var brokerAddresses []string
var topics []uint64

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
		case lineContents[0] == tagTopics:
			if len(lineContents) == 2 {
				topicStrings := strings.Split(lineContents[1], ",")
				for _, topicString := range topicStrings {
					tempTopic, err := strconv.ParseUint(topicString, 10, 64)
					if err != nil {
						return err
					}
					topics = append(topics, tempTopic)
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
