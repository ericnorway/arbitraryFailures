# arbitraryFailures

To start a broker replica node:
Run the server app with the following flags:
-config should be the name of the config file for that node.
-alpha should be the number of Chain or AB pubs to accept before requesting a BRB pub.
 0 means that you don't want to combine the algorithms.

Here is an example config file for a broker replica node:
ID=0
PUB_KEYS=11111111,22222222,33333333,44444444
SUB_KEYS=A1A1A1A1,B2B2B2B2,C3C3C3C3,D4D4D4D4
BRK_KEYS=        ,AAAAAAAA,BBBBBBBB,CCCCCCCC
BRK_ADDR=localhost:11110,localhost:11111,localhost:11112,localhost:11113

To start a subscriber:
Run the client app with the following flags:
-config should be the name of the config file for that subscriber. 
-type should be "subscriber".

Here is an example config file for a subscriber:
ID=0
BRK_KEYS=A1A1A1A1,E5E5E5E5,I9I9I9I9,O1O3O1O3
BRK_ADDR=localhost:11110,localhost:11111,localhost:11112,localhost:11113
TOPICS=1,2

To start a publisher:
Run the client app with the following flags:
-config should be the name of the config file for that publisher. 
-type should be "publisher".
-pubCount should be the number of publications to send.
-pubType should be the publication algorithm to use: AB, BRB, or Chain.

Here is an example config file for a publisher:
ID=0
BRK_KEYS=11111111,55555555,99999999,13131313
BRK_ADDR=localhost:11110,localhost:11111,localhost:11112,localhost:11113

Note that all keys are private (shared) keys. For example, broker 0 and publisher 0 share the key 11111111.
