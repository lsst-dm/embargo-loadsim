#!/bin/bash
# run from within config/ directory
# ./primary.sh realistic baseline-1
# loads `realistic.conf` overrides and adds 'baseline-1' to raw/writeout paths
ulimit -n 100000
locust --config ./$1.conf --detectors 189 --worker-pool 1200 \
    --logfile ../run-$2-debug.log \
    --master --expect-workers 64 --run-id $2
