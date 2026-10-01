#!/bin/bash
# BEFORE USE: set AWS keys and replace leaderhost below
ulimit -n 100000
export AWS_ACCESS_KEY_ID=""
export AWS_SECRET_ACCESS_KEY=""
export AWS_REGION="us-east-1"
locust -f - --processes 16 --worker --master-host leaderhost
