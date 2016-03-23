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
var tagRChain = "RCHAIN"
var localID uint64
var publisherKeys []string
var subscriberKeys []string
var brokerKeys []string
var brokerAddresses []string
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

	idFound := false
	pKeysFound := false
	sKeysFound := false
	bKeysFound := false
	bAddrFound := false

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
				idFound = true
			} else {
				return fmt.Errorf("Error parsing the config file entry for %s.\n", lineContents[0])
			}
		case lineContents[0] == tagPublisherKeys:
			if len(lineContents) == 2 {
				publisherKeys = strings.Split(lineContents[1], ",")
				pKeysFound = true
			} else {
				return fmt.Errorf("Error parsing the config file entry for %s.\n", lineContents[0])
			}
		case lineContents[0] == tagSubscriberKeys:
			if len(lineContents) == 2 {
				subscriberKeys = strings.Split(lineContents[1], ",")
				sKeysFound = true
			} else {
				return fmt.Errorf("Error parsing the config file entry for %s.\n", lineContents[0])
			}
		case lineContents[0] == tagBrokerKeys:
			if len(lineContents) == 2 {
				brokerKeys = strings.Split(lineContents[1], ",")
				bKeysFound = true
			} else {
				return fmt.Errorf("Error parsing the config file entry for %s.\n", lineContents[0])
			}
		case lineContents[0] == tagBrokerAddrs:
			if len(lineContents) == 2 {
				brokerAddresses = strings.Split(lineContents[1], ",")
				bAddrFound = true
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

				// If there are parents to the node
				if len(temp) == 2 {
					parents := strings.Split(temp[1], ",")
					if len(parents) != 0 && parents[0] != "" {
						rChain[node] = parents
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

	// Check that the required fields are found.
	if idFound == false {
		return fmt.Errorf("Please add %v=<id> to the config file.", tagID)
	}
	if pKeysFound == false {
		return fmt.Errorf("Please add %v=<key1>,<key2>,... to the config file.", tagPublisherKeys)
	}
	if sKeysFound == false {
		return fmt.Errorf("Please add %v=<key1>,<key2>,... to the config file.", tagSubscriberKeys)
	}
	if bKeysFound == false {
		return fmt.Errorf("Please add %v=<key1>,<key2>,... to the config file.", tagBrokerKeys)
	}
	if bAddrFound == false {
		return fmt.Errorf("Please add %v=<addr1>,<addr2>,... to the config file.", tagBrokerAddrs)
	}

	if len(brokerAddresses) != len(brokerKeys) {
		return fmt.Errorf("The number of broker addresses is not the same as the number of broker keys.")
	}

	return nil
}
