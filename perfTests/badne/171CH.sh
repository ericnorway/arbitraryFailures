#!/bin/bash

#variables
alpha=0
pubType=Chain
pubCount=3
baseResultsDir="/home/stud/ericfree/go/src/github.com/ericnorway/arbitraryFailures/results"
resultsDir="$baseResultsDir/badne"
configDir="/home/stud/ericfree/go/src/github.com/ericnorway/arbitraryFailures/configs/badne7brokers"
brokerCount=7
subscriberCount=1
publisherCount=1
maliciousPct=0
time=90

./test.sh $alpha $pubType $pubCount $baseResultsDir $resultsDir $configDir $brokerCount $subscriberCount $publisherCount $maliciousPct $time
