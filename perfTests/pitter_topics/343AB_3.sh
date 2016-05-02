#!/bin/bash

#variables
alpha=3
pubType=AB
pubCount=50000
baseResultsDir="/home/stud/ericfree/go/src/github.com/ericnorway/arbitraryFailures/results"
resultsDir="$baseResultsDir/pitter_topics"
configDir="/home/stud/ericfree/go/src/github.com/ericnorway/arbitraryFailures/configs/pitter_topics"
brokerCount=4
subscriberCount=3
publisherCount=3
maliciousPct=0
time=200

./test.sh $alpha $pubType $pubCount $baseResultsDir $resultsDir $configDir $brokerCount $subscriberCount $publisherCount $maliciousPct $time
