# arbitraryFailures

To start a broker replica node:<br/>
Run the server app with the following flags:<br/>
-config should be the name of the config file for that node.<br/>
-alpha should be number of AB or Chain pubs to accept before requesting a BRB pub (with history).<br/>
 0 means that you don't want to combine the algorithms.<br/>
-mal should be the percent of messages to maliciously alter.<br/>
 The default is 0.<br/>

Here is an example config file for a broker replica node:<br/>
ID=0<br/>
PUB_KEYS=11111111,22222222,33333333,44444444<br/>
SUB_KEYS=A1A1A1A1,B2B2B2B2,C3C3C3C3,D4D4D4D4<br/>
BRK_KEYS=        ,AAAAAAAA,BBBBBBBB,CCCCCCCC<br/>
BRK_ADDR=localhost:11110,localhost:11111,localhost:11112,localhost:11113<br/>
<br/>
CHAIN=B0:B1<br/>
CHAIN=B1:B2<br/>
CHAIN=B2:S0,S1,S2,S3<br/>
CHAIN=S0<br/>
CHAIN=S1<br/>
CHAIN=S2<br/>
CHAIN=S3<br/>
<br/>
RCHAIN=B0:P0,P1,P2,P3<br/>
RCHAIN=P0<br/>
RCHAIN=P1<br/>
RCHAIN=P2<br/>
RCHAIN=P3<br/>

To start a subscriber:<br/>
Run the client app with the following flags:<br/>
-config should be the name of the config file for that subscriber. <br/>
-type should be "subscriber".<br/>

Here is an example config file for a subscriber:<br/>
ID=0<br/>
BRK_KEYS=A1A1A1A1,E5E5E5E5,I9I9I9I9,O1O3O1O3<br/>
BRK_ADDR=localhost:11110,localhost:11111,localhost:11112,localhost:11113<br/>
TOPICS=1,2<br/>
<br/>
RCHAIN=S0:B2<br/>
RCHAIN=B2:B1<br/>
RCHAIN=B1:B0<br/>
RCHAIN=B0<br/>

To start a publisher:<br/>
Run the client app with the following flags:<br/>
-config should be the name of the config file for that publisher.<br/>
-type should be "publisher".<br/>
-pubCount should be the number of publications to send.<br/>
-pubType should be the publication algorithm to use: AB, BRB, or Chain.<br/>
-topics should be the number of topics to use.<br/>

Here is an example config file for a publisher:<br/>
ID=0<br/>
BRK_KEYS=11111111,55555555,99999999,13131313<br/>
BRK_ADDR=localhost:11110,localhost:11111,localhost:11112,localhost:11113<br/>
<br/>
CHAIN=P0:B0<br/>
CHAIN=B0:B1<br/>
CHAIN=B1:B2<br/>
CHAIN=B2<br/>

Note that all keys are private (shared) keys. For example, broker 0 and publisher 0 share the key 11111111.<br/>

CHAIN and RCHAIN specify the parts of the chain tree that the node knows about. CHAIN species children, while
RCHAIN specifies parents. So in the example of the publisher, 
it knows about nodes P0, B0, B1, and B2. P0 has child B0. B0 has child B1. B1 has child B2. While B2 has children, 
but the publisher does know about the subscribers, so it does not include B2's children of S0, S1, S2, S3.
