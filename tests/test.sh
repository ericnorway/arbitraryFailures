#!/bin/bash

#arguments
alpha=$1
pubType=$2
pubCount=$3
resultsDir=$4
testResultsDir=$5
configDir=$6
brokerCount=$7
subscriberCount=$8
publisherCount=$9
maliciousPct=${10}
time=${11}

#hosts for each machine
brokerMachines=(localhost localhost localhost localhost localhost localhost localhost localhost)
subscriberMachines=(localhost localhost localhost localhost localhost localhost localhost localhost)
publisherMachines=(localhost localhost localhost localhost localhost localhost localhost localhost)

#start brokers
if [ maliciousPct = 0 ]; then
	#normal brokers
	for ((id=0; id < brokerCount; id++)); do
		gnome-terminal -e "./broker.sh ${brokerMachines[$id]} $id $configDir $time $alpha $maliciousPct"
	done
else
	#malicious broker
	gnome-terminal -e "./broker.sh ${brokerMachines[0]} 0 $configDir $time $alpha 0"
	gnome-terminal -e "./broker.sh ${brokerMachines[1]} 1 $configDir $time $alpha $maliciousPct"
	gnome-terminal -e "./broker.sh ${brokerMachines[2]} 2 $configDir $time $alpha 0"
	gnome-terminal -e "./broker.sh ${brokerMachines[3]} 3 $configDir $time $alpha 0"
fi

#start subscribers
for ((id=0; id < subscriberCount; id++))
do
	gnome-terminal -e "./subscriber.sh ${subscriberMachines[$id]} $id $configDir $time"
done

#start publishers
for ((id=0; id < publisherCount; id++))
do
	gnome-terminal -e "./publisher.sh ${publisherMachines[$id]} $id $configDir $time $pubType $pubCount"
done

#wait for test to finish
sleep $time

#move the results to the appropriate directory
rm -r $testResultsDir
mkdir $testResultsDir
mv $resultsDir/*.txt $testResultsDir

go run ../checkOutputFiles/checkLatency.go -dir=$testResultsDir > $testResultsDir/LATENCY.txt
go run ../checkOutputFiles/checkThroughput.go -dir=$testResultsDir > $testResultsDir/THROUGHPUT.txt

