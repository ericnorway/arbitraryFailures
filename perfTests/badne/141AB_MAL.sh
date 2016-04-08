#!/bin/bash

#variables
alpha=10
pubType=AB
pubCount=30
resultsDir="/home/stud/ericfree/go/src/github.com/ericnorway/arbitraryFailures/results"
testResultsDir="$resultsDir/badne/141AB_MAL"
configDir="/home/stud/ericfree/go/src/github.com/ericnorway/arbitraryFailures/configs/badne""
brokerCount=4
subscriberCount=1
publisherCount=1
maliciousPct=10
time=150

./test.sh $alpha $pubType $pubCount $resultsDir $testResultsDir $configDir $brokerCount $subscriberCount $publisherCount $maliciousPct $time
