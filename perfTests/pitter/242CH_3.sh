#!/bin/bash

#variables
alpha=3
pubType=Chain
pubCount=10000
baseResultsDir="/home/stud/ericfree/go/src/github.com/ericnorway/arbitraryFailures/results"
resultsDir="$baseResultsDir/pitter"
configDir="/home/stud/ericfree/go/src/github.com/ericnorway/arbitraryFailures/configs/pitter"
brokerCount=4
subscriberCount=2
publisherCount=2
maliciousPct=0
time=300

./test.sh $alpha $pubType $pubCount $baseResultsDir $resultsDir $configDir $brokerCount $subscriberCount $publisherCount $maliciousPct $time
