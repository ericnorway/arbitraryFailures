#!/bin/bash

#variables
alpha=10
pubType=Chain
pubCount=30
baseResultsDir="/home/stud/ericfree/go/src/github.com/ericnorway/arbitraryFailures/results"
resultsDir="$resultsDir/badne"
configDir="/home/stud/ericfree/go/src/github.com/ericnorway/arbitraryFailures/configs/badne"
brokerCount=4
subscriberCount=1
publisherCount=1
maliciousPct=25
time=150

./test.sh $alpha $pubType $pubCount $baseResultsDir $resultsDir $configDir $brokerCount $subscriberCount $publisherCount $maliciousPct $time
