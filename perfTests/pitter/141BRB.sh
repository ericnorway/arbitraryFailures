#!/bin/bash

#variables
alpha=0
pubType=BRB
pubCount=100
resultsDir="/home/stud/ericfree/go/src/github.com/ericnorway/arbitraryFailures/results"
testResultsDir="$resultsDir/pitter/141BRB"
configDir="/home/stud/ericfree/go/src/github.com/ericnorway/arbitraryFailures/configs/pitter"
brokerCount=4
subscriberCount=1
publisherCount=1
maliciousPct=0
time=150

./test.sh $alpha $pubType $pubCount $resultsDir $testResultsDir $configDir $brokerCount $subscriberCount $publisherCount $maliciousPct $time
