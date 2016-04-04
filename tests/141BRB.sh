#!/bin/bash

#variables
alpha=0
pubType=BRB
pubCount=100
resultsDir="../results"
testResultsDir="$resultsDir/141BRBT"
configDir="./env/badne"
brokerCount=4
subscriberCount=1
publisherCount=1
maliciousPct=0
time=150

./test.sh $alpha $pubType $pubCount $resultsDir $testResultsDir $configDir $brokerCount $subscriberCount $publisherCount $maliciousPct $time
