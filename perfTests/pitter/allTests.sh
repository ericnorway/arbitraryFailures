#!/bin/bash

# SHOULD BE RUN DIRECTLY FROM ITS DIRECTORY

# 1 publisher, 4 brokers. 1 subscriber
./141AB.sh
./141AB_3.sh
./141AB_5.sh
./141AB_10.sh
./141BRB.sh
./141CH.sh
./141CH_3.sh
./141CH_5.sh
./141CH_10.sh

# 2 publishers, 4 brokers. 2 subscribers
./242AB.sh
./242AB_3.sh
./242AB_5.sh
./242AB_10.sh
./242BRB.sh
./242CH.sh
./242CH_3.sh
./242CH_5.sh
./242CH_10.sh

# 3 publishers, 4 brokers. 3 subscribers
./343AB.sh
./343AB_3.sh
./343AB_5.sh
./343AB_10.sh
./343BRB.sh
./343CH.sh
./343CH_3.sh
./343CH_5.sh
./343CH_10.sh

# 4 publishers, 4 brokers. 4 subscribers
./444AB.sh
./444AB_3.sh
./444AB_5.sh
./444AB_10.sh
./444BRB.sh
./444CH.sh
./444CH_3.sh
./444CH_5.sh
./444CH_10.sh

# 4 publishers, 4 brokers. 4 subscribers
./545AB.sh
./545AB_3.sh
./545AB_5.sh
./545AB_10.sh
./545BRB.sh
./545CH.sh
./545CH_3.sh
./545CH_5.sh
./545CH_10.sh

# 6 publishers, 4 brokers. 6 subscribers
./646AB.sh
./646AB_3.sh
./646AB_5.sh
./646AB_10.sh
./646BRB.sh
./646CH.sh
./646CH_3.sh
./646CH_5.sh
./646CH_10.sh

# 8 publishers, 4 brokers. 8 subscribers
./848AB.sh
./848AB_3.sh
./848AB_5.sh
./848AB_10.sh
./848BRB.sh
./848CH.sh
./848CH_3.sh
./848CH_5.sh
./848CH_10.sh

# 10 publishers, 4 brokers, 10 subscribers
./10410AB.sh
./10410AB_3.sh
./10410AB_5.sh
./10410AB_10.sh
./10410BRB.sh
./10410CH.sh
./10410CH_3.sh
./10410CH_5.sh
./10410CH_10.sh

# 12 publishers, 4 brokers, 12 subscribers
./12412AB.sh
./12412AB_3.sh
./12412AB_5.sh
./12412AB_10.sh
./12412BRB.sh
./12412CH.sh
./12412CH_3.sh
./12412CH_5.sh
./12412CH_10.sh

# 4 publishers, 7 brokers, 4 subscribers
#./474AB.sh
#./474AB_3.sh
#./474AB_5.sh
#./474AB_10.sh
#./474BRB.sh
#./474CH.sh
#./474CH_3.sh
#./474CH_5.sh
#./474CH_10.sh

# 4 publishers, 4 brokers, 4 subscribers - one malicious broker
#./444AB_MAL.sh
#./444AB_3_MAL.sh
#./444AB_5_MAL.sh
#./444AB_10_MAL.sh
#./444BRB_MAL.sh
#./444CH_MAL.sh
#./444CH_3_MAL.sh
#./444CH_5_MAL.sh
#./444CH_10_MAL.sh

# 4 publishers, 4 brokers, 4 subscribers - multiple topics
#./444AB_T.sh
#./444AB_3_T.sh
#./444AB_5_T.sh
#./444AB_10_T.sh
#./444BRB_T.sh
#./444CH_T.sh
#./444CH_3_T.sh
#./444CH_5_T.sh
#./444CH_10_T.sh
