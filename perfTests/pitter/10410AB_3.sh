#!/bin/bash

#variables
alpha=3
pubType=AB
pubCount=10000
baseResultsDir="/home/stud/ericfree/go/src/github.com/ericnorway/arbitraryFailures/results"
resultsDir="$baseResultsDir/pitter"
configDir="/home/stud/ericfree/go/src/github.com/ericnorway/arbitraryFailures/configs/pitter"
brokerCount=4
subscriberCount=10
publisherCount=10
maliciousPct=0
time=360

./test.sh $alpha $pubType $pubCount $baseResultsDir $resultsDir $configDir $brokerCount $subscriberCount $publisherCount $maliciousPct $time
