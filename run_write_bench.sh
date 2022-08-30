#!/bin/env bash

bin/app \
  --service-url="192.168.0.216:6570" \
  --stream-count 1 \
  --thread-count 1 \
  --record-size 1024 \
  --record-type="raw" \
  --rate-limit 500000 \
  --stream-backlog-duration=600 \
  --stream-replication-factor=1 \
  --batch-bytes-limit=819200 \
  --batch-age-limit=-1 \
  --total-bytes-limit=81920000
