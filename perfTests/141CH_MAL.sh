#!/bin/bash

#variables
alpha=10
pubType=Chain
pubCount=30
resultsDir="../results"
testResultsDir="$resultsDir/141CH_MAL"
configDir="./env/badne"
brokerCount=4
subscriberCount=1
publisherCount=1
maliciousPct=25
time=150

./test.sh $alpha $pubType $pubCount $resultsDir $testResultsDir $configDir $brokerCount $subscriberCount $publisherCount $maliciousPct $time
