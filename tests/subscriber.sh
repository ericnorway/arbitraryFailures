#!/bin/bash

host=$1
id=$2
configDir=$3
time=$4

#wait for brokers to start
sleep 10

#open as background process so it can be killed later
fab -H $host subscriber:id=$id,configDir=$configDir --disable-known-hosts &
id = $!
sleep `expr $time - 25`
kill $id
