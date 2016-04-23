#!/bin/bash

#arguments
alpha=$1
pubType=$2
pubCount=$3
baseResultsDir=$4
resultsDir=$5
configDir=$6
brokerCount=$7
subscriberCount=$8
publisherCount=$9
maliciousPct=${10}
time=${11}

#hosts for each machine
brokerMachines=(pitter30 pitter31 pitter32 pitter33 pitter34 pitter35 pitter36)
subscriberMachines=(pitter26 pitter27 pitter39 pitter29 pitter26 pitter27 pitter39 pitter29 pitter26 pitter27 pitter39 pitter29 pitter26 pitter27)
publisherMachines=(pitter26 pitter27 pitter39 pitter29 pitter26 pitter27 pitter39 pitter29 pitter26 pitter27 pitter39 pitter29 pitter26 pitter27)

#start brokers
if [ $maliciousPct = 0 ]; then
	#normal brokers
	for ((id=0; id < brokerCount; id++)); do
		gnome-terminal -e "../broker.sh ${brokerMachines[$id]} $id $configDir $time $alpha $maliciousPct"
	done
else
	#malicious broker
	# DON'T FORGET TO USE DIFFERENT CONFIG FOR MALICIOUS TESTS... For malicious AB tests to work properly,
	# the publisher must also misbehave and ignore one of the brokers.
	gnome-terminal -e "../broker.sh ${brokerMachines[0]} 0 $configDir $time $alpha 0"
	gnome-terminal -e "../broker.sh ${brokerMachines[1]} 1 $configDir $time $alpha $maliciousPct"
	gnome-terminal -e "../broker.sh ${brokerMachines[2]} 2 $configDir $time $alpha 0"
	gnome-terminal -e "../broker.sh ${brokerMachines[3]} 3 $configDir $time $alpha 0"
	gnome-terminal -e "../broker.sh ${brokerMachines[4]} 4 $configDir $time $alpha 0"
	gnome-terminal -e "../broker.sh ${brokerMachines[5]} 5 $configDir $time $alpha $maliciousPct"
	gnome-terminal -e "../broker.sh ${brokerMachines[6]} 6 $configDir $time $alpha 0"
fi

#start subscribers
for ((id=0; id < subscriberCount; id++))
do
	gnome-terminal -e "../subscriber.sh ${subscriberMachines[$id]} $id $configDir $time"
done

#start publishers
for ((id=0; id < publisherCount; id++))
do
	gnome-terminal -e "../publisher.sh ${publisherMachines[$id]} $id $configDir $time $pubType $pubCount"
done

#wait for test to finish
sleep $time

if [ $maliciousPct = 0 ]; then
	malPart=""
else
	malPart=_MAL
fi

if [ $alpha = 0 ]; then
	alphaPart=""
else
	alphaPart=_$alpha
fi

testResultsDir=$resultsDir/$publisherCount$brokerCount$subscriberCount$pubType$alphaPart$malPart

#move the results to the appropriate directory
rm -r $testResultsDir
mkdir $testResultsDir
mv $baseResultsDir/*.txt $testResultsDir

# Calculate the throughput and latency
../../checkOutputFiles/checkOutputFiles -dir=$testResultsDir > $testResultsDir/RESULTS.txt
