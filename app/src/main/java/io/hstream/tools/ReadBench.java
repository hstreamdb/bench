package io.hstream.tools;

import com.google.common.util.concurrent.RateLimiter;
import io.hstream.BatchSetting;
import io.hstream.BufferedProducer;
import io.hstream.Consumer;
import io.hstream.FlowControlSetting;
import io.hstream.HStreamClient;
import io.hstream.Record;
import io.hstream.Subscription;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class ReadBench {

  private static long lastReportTs;
  private static long lastReadSuccessReads;

  private static AtomicLong successReads = new AtomicLong();

  public static void main(String[] args) throws Exception {
    var streamName = UUID.randomUUID().toString();
    var serviceUrl = "192.168.0.216:6570";
    HStreamClient client = HStreamClient.builder().serviceUrl(serviceUrl).build();
    client.createStream(streamName, (short) 1, 60 * 60);
    long totalSize = 100L * 1024 * 1024 * 1024;
    int recordSize = 1024;
    int writeRate = 500 * 1024 * 1024 / recordSize;
    int keyCount = 20;
    int batchSize = 819200;
    write(client, streamName, writeRate, totalSize, recordSize, keyCount, batchSize);

    System.out.println(String.format("wrote done"));

    new Thread(() -> read(client, streamName)).start();

    lastReportTs = System.currentTimeMillis();
    lastReadSuccessReads = 0;
    int reportIntervalSeconds = 3;
    while (true) {
      Thread.sleep(reportIntervalSeconds * 1000);
      long now = System.currentTimeMillis();
      long successRead = successReads.get();
      long duration = now - lastReportTs;
      double successPerSeconds = (double) (successRead - lastReadSuccessReads) * 1000 / duration;
      double throughput =
          (double) (successRead - lastReadSuccessReads)
              * recordSize
              * 1000
              / duration
              / 1024
              / 1024;

      lastReportTs = now;
      lastReadSuccessReads = successRead;

      System.out.println(
          String.format("[Read]: %f record/s, throughput %f MB/s", successPerSeconds, throughput));
    }
  }

  static void write(
      HStreamClient client,
      String streamName,
      int writeRate,
      long totalSize,
      int recordSize,
      int keyCount,
      int batchSize) {
    BufferedProducer bufferedProducer =
        client.newBufferedProducer().stream(streamName)
            .batchSetting(
                BatchSetting.newBuilder()
                    .bytesLimit(batchSize)
                    .ageLimit(-1)
                    .recordCountLimit(-1)
                    .build())
            .flowControlSetting(FlowControlSetting.newBuilder().bytesLimit(batchSize * 100).build())
            .build();
    Random random = new Random();
    byte[] payload = new byte[recordSize];
    random.nextBytes(payload);
    RateLimiter rateLimiter = RateLimiter.create(writeRate);
    for (long i = 0; i < totalSize / recordSize; ) {
      for (int j = 0; j < keyCount; ++j, ++i) {
        rateLimiter.acquire();
        bufferedProducer.write(
            Record.newBuilder().orderingKey("key-" + j).rawRecord(payload).build());
      }
    }
    bufferedProducer.flush();
    bufferedProducer.close();
  }

  static void read(HStreamClient client, String streamName) {
    var subscriptionId = UUID.randomUUID().toString();
    client.createSubscription(
        Subscription.newBuilder().stream(streamName)
            .subscription(subscriptionId)
            .ackTimeoutSeconds(6000)
            .build());
    Consumer consumer =
        client
            .newConsumer()
            .subscription(subscriptionId)
            .rawRecordReceiver(
                (record, responder) -> {
                  successReads.incrementAndGet();
                  responder.ack();
                })
            .build();
    consumer.startAsync().awaitRunning();
    consumer.awaitTerminated();
  }
}
