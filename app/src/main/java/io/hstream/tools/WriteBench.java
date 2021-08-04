package io.hstream.tools;

import io.hstream.*;
import io.hstream.ClientBuilder;

import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class WriteBench {

    private static final String serviceUrl = "localhost:6570";
    private static final String streamName = "write_bench_stream";
    private static final long APPEND_COUNT = 1000000000;
    private static final int RECORD_SIZE = 100;
    private static final int BATCH_SIZE = 1000;

    private static AtomicLong appendedCounter = new AtomicLong();
    private static AtomicLong lastReportTs = new AtomicLong();
    private static AtomicLong lastReadCount = new AtomicLong();
    private static ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1);

    public static void main(String[] args) {
        HStreamClient client = HStreamClient.builder().serviceUrl(serviceUrl).build();

        Producer producer = client.newProducer().stream(streamName).enableBatch().recordCountLimit(BATCH_SIZE).build();
        Random random = new Random();
        byte[] payload = new byte[RECORD_SIZE];
        random.nextBytes(payload);

        lastReportTs.set(System.currentTimeMillis());
        lastReadCount.set(0);
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            long countNow = appendedCounter.get();
            long duration = now - lastReportTs.get();
            long countDuration = countNow - lastReadCount.get();
            double result = (double)countDuration * 1000 / duration;
            System.out.println("write " + result + " msg/s");
            lastReadCount.set(countNow);
            lastReportTs.set(now);
        } , 0, 3, TimeUnit.SECONDS);

        for(long i = 0; i < APPEND_COUNT; ++i) {
            producer.writeAsync(payload).thenRun(() -> appendedCounter.incrementAndGet());
        }

    }
}
