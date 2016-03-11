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
var tagChain = "CHAIN"
var tagRChain = "RCHAIN"
var localID uint64
var brokerKeys []string
var brokerAddresses []string
var topics []uint64
var chain map[string][]string
var rChain map[string][]string

// ReadConfigFile reads a config file and updated the values of global variables.
// It returns an error, if any.
func ReadConfigFile(fileName string) error {
	// Read the whole file
	bytes, err := ioutil.ReadFile(fileName)
	if err != nil {
		return err
	}

	chain = make(map[string][]string)
	rChain = make(map[string][]string)

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
		case lineContents[0] == tagChain:
			// If there is a value to the tag
			if len(lineContents) == 2 {
				temp := strings.Split(lineContents[1], ":")
				node := temp[0]

				// If there are children to the node
				if len(temp) == 2 {
					children := strings.Split(temp[1], ",")
					if len(children) != 0 && children[0] != "" {
						chain[node] = children
					} else {
						chain[node] = nil
					}
				} else {
					chain[node] = nil
				}
			} else {
				return fmt.Errorf("Error parsing the config file entry for %s.\n", lineContents[0])
			}
		case lineContents[0] == tagRChain:
			// If there is a value to the tag
			if len(lineContents) == 2 {
				temp := strings.Split(lineContents[1], ":")
				node := temp[0]

				// If there are children to the node
				if len(temp) == 2 {
					children := strings.Split(temp[1], ",")
					if len(children) != 0 && children[0] != "" {
						rChain[node] = children
					} else {
						rChain[node] = nil
					}
				} else {
					rChain[node] = nil
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
