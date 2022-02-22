package io.hstream.tools;

import io.hstream.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import com.google.common.util.concurrent.RateLimiter;

public class WriteBench {

    private static final String serviceUrl = "localhost:6570";
    private static final String streamNamePrefix = "write_bench_stream_";

    private static int recordSize = 1024; // bytes
    private static int flushMills = 10; // ms
    private static int bufferSize = 1024 * 1024; // bytes
    private static int streamCount = 100;
    private static int threadCount = 4;
    private static int reportIntervalSeconds = 3;
    private static int rateLimit = 100000;

    private static ExecutorService executorService;

    private static long lastReportTs;
    private static long lastReadSuccessAppends;
    private static long lastReadFailedAppends;

    private static AtomicLong successAppends = new AtomicLong();
    private static AtomicLong failedAppends = new AtomicLong();

    public static void main(String[] args) throws Exception{
        HStreamClient client = HStreamClient.builder().serviceUrl(serviceUrl).build();
        List<List<BufferedProducer>> producersPerThread = new ArrayList<>(threadCount);
        executorService = Executors.newFixedThreadPool(threadCount);
        RateLimiter rateLimiter = RateLimiter.create(rateLimit);

        for(int i = 0; i < streamCount; ) {
            List<BufferedProducer> bufferedProducers = new ArrayList<>(streamCount / threadCount);
            for(int j = 0; j < threadCount; ++j, ++i) {
                var streamName = streamNamePrefix + i;
                client.createStream(streamName);
                var bufferedProducer = client.newBufferedProducer().stream(streamName).maxBytesSize(bufferSize).flushIntervalMs(flushMills).recordCountLimit(rateLimit * 2 / (1000 / flushMills)).build();
                bufferedProducers.add(bufferedProducer);
            }
            producersPerThread.add(bufferedProducers);
        }

        Random random = new Random();
        byte[] payload = new byte[recordSize];
        random.nextBytes(payload);
        Record record = Record.newBuilder().rawRecord(payload).build();

        lastReportTs = System.currentTimeMillis();
        lastReadSuccessAppends = 0;
        lastReadFailedAppends = 0;
        for(int i = 0; i < threadCount; ++i) {
            int index = i;
            executorService.submit(() -> {
                append(rateLimiter, producersPerThread.get(index), record);
            });
        }

        while(true) {
            Thread.sleep(reportIntervalSeconds * 1000);
            long now = System.currentTimeMillis();
            long successRead = successAppends.get();
            long failedRead = failedAppends.get();
            long duration = now - lastReportTs;
            double successPerSeconds = (double)(successRead - lastReadSuccessAppends) * 1000 / duration;
            double failurePerSeconds = (double)(failedRead - lastReadFailedAppends) * 1000 / duration;
            double throughput = (double)(successRead - lastReadSuccessAppends) * recordSize * 1000 / duration / 1024 / 1024;

            lastReportTs = now;
            lastReadSuccessAppends = successRead;
            lastReadFailedAppends = failedRead;

            System.out.println(String.format("[Append]: success %f record/s, failed %f record/s, throughput %f MB/s", successPerSeconds, failurePerSeconds, throughput));
        }
    }

    public static void append(RateLimiter rateLimiter, List<BufferedProducer> producers, Record record) {
        while(true) {

            for(var producer: producers) {
               rateLimiter.acquire();
               producer.write(record).handle((recordId, throwable) -> {
                    if(throwable != null) {
                        failedAppends.incrementAndGet();
                    } else {
                        successAppends.incrementAndGet();
                    }
                    return null;
               });
            }

            // Thread.yield();
        }

    }
}
