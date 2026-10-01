#!/bin/bash
ulimit -n 100000
# Ensure AWS variables are set
locust --config ./realistic.conf --warm --detectors 189 --worker-pool 800
