#!/bin/bash

#variables
alpha=0
pubType=Chain
pubCount=100
baseResultsDir="/home/stud/ericfree/go/src/github.com/ericnorway/arbitraryFailures/results"
resultsDir="$resultsDir/badne"
configDir="/home/stud/ericfree/go/src/github.com/ericnorway/arbitraryFailures/configs/badne"
brokerCount=4
subscriberCount=1
publisherCount=1
maliciousPct=0
time=150

./test.sh $alpha $pubType $pubCount $baseResultsDir $resultsDir $configDir $brokerCount $subscriberCount $publisherCount $maliciousPct $time
