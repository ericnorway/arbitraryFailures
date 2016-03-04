# arbitraryFailures

To start a broker replica node:<br/>
Run the server app with the following flags:<br/>
-config should be the name of the config file for that node.<br/>
-alpha should be number of AB or Chain pubs to accept before requesting a BRB pub (with history).<br/>
 0 means that you don't want to combine the algorithms.<br/>

Here is an example config file for a broker replica node:<br/>
ID=0<br/>
PUB_KEYS=11111111,22222222,33333333,44444444<br/>
SUB_KEYS=A1A1A1A1,B2B2B2B2,C3C3C3C3,D4D4D4D4<br/>
BRK_KEYS=        ,AAAAAAAA,BBBBBBBB,CCCCCCCC<br/>
BRK_ADDR=localhost:11110,localhost:11111,localhost:11112,localhost:11113<br/>

To start a subscriber:<br/>
Run the client app with the following flags:<br/>
-config should be the name of the config file for that subscriber. <br/>
-type should be "subscriber".<br/>

Here is an example config file for a subscriber:<br/>
ID=0<br/>
BRK_KEYS=A1A1A1A1,E5E5E5E5,I9I9I9I9,O1O3O1O3<br/>
BRK_ADDR=localhost:11110,localhost:11111,localhost:11112,localhost:11113<br/>
TOPICS=1,2<br/>

To start a publisher:<br/>
Run the client app with the following flags:<br/>
-config should be the name of the config file for that publisher.<br/>
-type should be "publisher".<br/>
-pubCount should be the number of publications to send.<br/>
-pubType should be the publication algorithm to use: AB, BRB, or Chain.<br/>

Here is an example config file for a publisher:<br/>
ID=0<br/>
BRK_KEYS=11111111,55555555,99999999,13131313<br/>
BRK_ADDR=localhost:11110,localhost:11111,localhost:11112,localhost:11113<br/>

Note that all keys are private (shared) keys. For example, broker 0 and publisher 0 share the key 11111111.<br/>
