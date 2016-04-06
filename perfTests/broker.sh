#!/bin/bash

host=$1
id=$2
configDir=$3
time=$4
alpha=$5
maliciousPct=$6

#open as background process so it can be killed later
fab -H $1 broker:id=$id,configDir=$configDir,alpha=$alpha,maliciousPct=$maliciousPct --disable-known-hosts &
pid=$!
sleep `expr $time - 10`
kill $pid
