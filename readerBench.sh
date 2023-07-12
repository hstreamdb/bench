#! /bin/bash

# docker run -td --name readBench --network host -v /root/bench:/root/bench -w /root/bench bench ./gradlew readBench --args='--service-url hstream://172.20.116.77 --record-size 1024 --bench-time -1 --warmup 1 --streams streams.txt --offset latest'

nohup ./gradlew readerBench --args='--service-url hstream://172.25.106.196 --record-size 1024  --bench-time -1 --warmup 30 --streams streams.txt --offset latest --report-interval 60' >reader_log 2>&1 &