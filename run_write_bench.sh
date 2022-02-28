#!/bin/env bash

./app \
  --service-url="192.168.0.216:6570" \
  --stream-count 320 \
  --thread-count 16 \
  --record-size 1024 \
  --rate-limit 500000 \
  --stream-name-prefix="ff_" \
  --buffer-size=819200 \
  --time-trigger=10 
