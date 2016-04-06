#!/bin/bash

#variables
alpha=0
pubType=AB
pubCount=100
resultsDir="/home/stud/ericfree/go/src/github.com/ericnorway/arbitraryFailures/results"
testResultsDir="$resultsDir/141ABT"
configDir="./env/badne"
brokerCount=4
subscriberCount=1
publisherCount=1
maliciousPct=0
time=150

./test.sh $alpha $pubType $pubCount $resultsDir $testResultsDir $configDir $brokerCount $subscriberCount $publisherCount $maliciousPct $time