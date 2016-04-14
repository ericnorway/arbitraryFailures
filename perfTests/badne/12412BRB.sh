#!/bin/bash

#variables
alpha=0
pubType=BRB
pubCount=1
baseResultsDir="/home/stud/ericfree/go/src/github.com/ericnorway/arbitraryFailures/results"
resultsDir="$baseResultsDir/badne"
configDir="/home/stud/ericfree/go/src/github.com/ericnorway/arbitraryFailures/configs/badne"
brokerCount=4
subscriberCount=12
publisherCount=12
maliciousPct=0
time=90

./test.sh $alpha $pubType $pubCount $baseResultsDir $resultsDir $configDir $brokerCount $subscriberCount $publisherCount $maliciousPct $time
