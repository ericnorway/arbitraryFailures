#!/bin/bash

host=$1
id=$2
configDir=$3
time=$4
pubType=$5
pubCount=$6

#wait for brokers and subscribers to start
sleep 30

#open as background process so it can be killed later.
#shouldn't need to kill it, but just it case.
fab -H $host publisher:id=$id,configDir=$configDir,pubType=$pubType,pubCount=$pubCount --disable-known-hosts &
pid=$!
sleep `expr $time - 50`
kill $pid