#!/bin/env bash

./app/build/install/app/bin/app \
  --service-url="127.0.0.1:6570" \
  --stream-count 1 \
  --thread-count 1 \
  --record-size 1024 \
  --rate-limit 500000 \
  --stream-name-prefix="ff_" \
  --buffer-size=819200 \
  --time-trigger=10 \
  --ordering-keys=5
