#!/bin/bash

java -jar sqlancer-*.jar \
	--num-tries 100000 \
	--timeout-seconds 600 \
	--print-progress-summary true \
	--num-threads 4 \
	sqlite3 --oracle Subset

