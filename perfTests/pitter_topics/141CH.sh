#!/bin/bash

#variables
alpha=0
pubType=Chain
pubCount=50000
baseResultsDir="/home/stud/ericfree/go/src/github.com/ericnorway/arbitraryFailures/results"
resultsDir="$baseResultsDir/pitter_topics"
configDir="/home/stud/ericfree/go/src/github.com/ericnorway/arbitraryFailures/configs/pitter_topics"
brokerCount=4
subscriberCount=1
publisherCount=1
maliciousPct=0
time=210

./test.sh $alpha $pubType $pubCount $baseResultsDir $resultsDir $configDir $brokerCount $subscriberCount $publisherCount $maliciousPct $time
