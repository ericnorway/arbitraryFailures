#!/bin/bash

#variables
alpha=10
pubType=AB
pubCount=30
baseResultsDir="/home/stud/ericfree/go/src/github.com/ericnorway/arbitraryFailures/results"
resultsDir="$baseResultsDir/badne"
configDir="/home/stud/ericfree/go/src/github.com/ericnorway/arbitraryFailures/configs/badne""
brokerCount=4
subscriberCount=1
publisherCount=1
maliciousPct=10
time=90

./test.sh $alpha $pubType $pubCount $baseResultsDir $resultsDir $configDir $brokerCount $subscriberCount $publisherCount $maliciousPct $time
