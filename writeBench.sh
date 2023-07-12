#! /bin/bash

# docker run -td --name writeBench --network host -v /root/bench:/root/bench -w /root/bench bench ./gradlew writeBench --args='--service-url hstream://172.20.116.77 --stream-count 32 --thread-count 16 --shard-count 1 --record-size 1024 --rate-limit 500000 --batch-bytes-limit 327680 --batch-age-limit -1 --total-bytes-limit 329000 --bench-time -1 --warmup 1 --compression none --fixed-stream-name --persistent-stream-path streams.txt'

# docker run -td --name writeBench --network host -v /root/bench:/root/bench -w /root/bench bench ./gradlew writeBench --args='--service-url hstream://172.20.116.77 --stream-count 64 --stream-backlog-duration 7200 --thread-count 32 --shard-count 1 --record-size 1024 --rate-limit 900000 --batch-bytes-limit 327680 --batch-age-limit -1 --total-bytes-limit 1024000 --bench-time -1 --warmup 1 --compression none --fixed-stream-name --persistent-stream-path streams.txt'

nohup ./gradlew writeBench --args='--service-url hstream://172.23.102.164 --stream-count 32 --stream-backlog-duration 7200 --thread-count 16 --shard-count 1 --record-size 1024 --rate-limit 300000 --batch-bytes-limit 327680 --batch-age-limit -1 --total-bytes-limit 1024000 --bench-time -1 --warmup 30 --compression none --fixed-stream-name --persistent-stream-path streams.txt --not-create-stream --report-interval 60' >write_log 2>&1 &

# ./gradlew createConnector --args='--service-url hstream://172.25.43.216 --connector-type SINK --target blackhole --thread-count 8 --streams streams.txt --clear'
