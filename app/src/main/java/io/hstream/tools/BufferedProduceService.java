package io.hstream.tools;

import com.google.common.util.concurrent.RateLimiter;
import io.grpc.Status;
import io.hstream.BufferedProducer;
import io.hstream.HStreamClient;
import io.hstream.Record;
import io.hstream.tools.Stats.Stats;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class BufferedProduceService {
  private final List<List<BufferedProducer>> producersPerThread;
  private final ExecutorService service;
  private final RateLimiter limiter;
  private final Utils.PayloadType payloadType;
  private final int recordSize;
  private final int partitionKeys;
  private static final AtomicBoolean terminateFlag = new AtomicBoolean(false);

  public BufferedProduceService(
      HStreamClient client,
      List<String> streams,
      int threadCount,
      int rateLimit,
      Utils.BufferedProducerOpts opts,
      Utils.PayloadOpts payloadOpts) {
    this.service = Executors.newFixedThreadPool(threadCount);
    this.limiter = RateLimiter.create(rateLimit);
    this.payloadType = payloadOpts.payloadType;
    this.recordSize = payloadOpts.recordSize;
    this.partitionKeys = payloadOpts.partitionKeys;

    var size = Math.min(threadCount, streams.size());
    this.producersPerThread = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      producersPerThread.add(new ArrayList<>());
    }

    var compresstionType = Utils.getCompressionType(opts.compTp);
    for (int i = 0; i < streams.size(); i++) {
      var bufferedProducer =
          Utils.buildBufferedProducer(client, streams.get(i), compresstionType, opts);
      producersPerThread.get(i % size).add(bufferedProducer);
    }
  }

  public void startService(AtomicBoolean warmupDone, Stats stats) {
    for (var producers : producersPerThread) {
      service.submit(() -> writeWithStats(producers, warmupDone, stats));
    }
  }

  public void writeNBytes(long totalSize) throws InterruptedException {
    int threads = producersPerThread.size();
    CountDownLatch latch = new CountDownLatch(threads);
    for (int i = 0; i < threads; i++) {
      final long size;
      if (i == threads - 1) {
        size = totalSize;
      } else {
        size = totalSize / threads;
        totalSize -= size;
      }
      int finalI = i;
      service.submit(() -> write(producersPerThread.get(finalI), latch, size));
    }
    latch.await();
    service.shutdown();
    service.awaitTermination(15, TimeUnit.SECONDS);
  }

  public void stopProducer() {
    terminateFlag.set(true);
  }

  public void stopService() throws InterruptedException {
    service.shutdown();
    service.awaitTermination(15, TimeUnit.SECONDS);
  }

  private void writeWithStats(
      List<BufferedProducer> producers, AtomicBoolean warmupDone, Stats stats) {
    Random random = new Random();
    Record record = Utils.makeRecord(payloadType, recordSize);
    while (true) {
      if (terminateFlag.get()) {
        stopProducers(producers);
        return;
      }

      for (var producer : producers) {
        limiter.acquire();
        String key = "test_" + random.nextInt(partitionKeys);
        record.setPartitionKey(key);
        final long sendTime = System.nanoTime();
        producer
            .write(record)
            .handle(
                (recordId, throwable) -> {
                  if (!warmupDone.get()) {
                    return null;
                  }

                  if (throwable != null) {
                    var status = Status.fromThrowable(throwable.getCause());
                    var errorCode = status.getCode();
                    if (errorCode == Status.UNAVAILABLE.getCode()
                        || errorCode == Status.DEADLINE_EXCEEDED.getCode()) {
                      stats.recordSendFailed();
                    } else {
                      System.exit(1);
                    }
                  } else {
                    long latencyMicros =
                        TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - sendTime);
                    stats.recordMessageSend(recordSize, latencyMicros);
                  }
                  return null;
                });
      }
    }
  }

  private void write(List<BufferedProducer> producers, CountDownLatch latch, long totalSize) {
    try {
      Random random = new Random();
      Record record = Utils.makeRecord(payloadType, recordSize);
      long writeCount = totalSize / recordSize;
      while (writeCount > 0) {
        for (var producer : producers) {
          limiter.acquire();
          String key = "test_" + random.nextInt(partitionKeys);
          record.setPartitionKey(key);
          producer.write(record);
          writeCount--;
        }
      }
    } finally {
      stopProducers(producers);
      latch.countDown();
    }
  }

  private void stopProducers(List<BufferedProducer> producers) {
    for (var producer : producers) {
      producer.flush();
      producer.close();
    }
  }
}
