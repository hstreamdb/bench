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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import picocli.CommandLine;

public class ReadBench {

  private static long lastReportTs;
  private static long lastReadSuccessReads;

  private static final AtomicLong successReads = new AtomicLong();
  private static final AtomicBoolean warmupDone = new AtomicBoolean(false);

  public static void main(String[] args) throws Exception {
    var options = new Options();
    var commandLine = new CommandLine(options).parseArgs(args);
    System.out.println(options);

    if (options.helpRequested) {
      CommandLine.usage(options, System.out);
      return;
    }

    if (options.warm >= options.benchmarkDuration) {
      System.err.println("Warmup time must be less than benchmark duration");
      System.exit(1);
    }

    var streamName = options.streamNamePrefix + UUID.randomUUID();
    HStreamClient client = HStreamClient.builder().serviceUrl(options.serviceUrl).build();
    client.createStream(streamName, options.streamReplicationFactor, options.streamBacklogDuration);
    int writeRate = 500 * 1024 * 1024 / options.recordSize;
    write(
        client,
        streamName,
        writeRate,
        options.totalWriteSize,
        options.recordSize,
        options.orderingKeys,
        options.batchSize);

    System.out.println("wrote done");

    var consumer = read(client, streamName);

    if (options.warm > 0) {
      System.out.println("Warmup ...... ");
      Thread.sleep(options.warm * 1000L);
      warmupDone.set(true);
    }

    lastReportTs = System.currentTimeMillis();
    var terminateTs = lastReportTs + options.benchmarkDuration * 1000L;
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
              * options.recordSize
              * 1000
              / duration
              / 1024
              / 1024;

      lastReportTs = now;
      lastReadSuccessReads = successRead;

      System.out.println(
          String.format("[Read]: %f record/s, throughput %f MB/s", successPerSeconds, throughput));
      if (terminateTs <= now) {
        break;
      }
    }
    consumer.stopAsync().awaitTerminated();
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

  static Consumer read(HStreamClient client, String streamName) {
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
                  if (warmupDone.get()) {
                    successReads.incrementAndGet();
                  }
                  responder.ack();
                })
            .build();
    consumer.startAsync().awaitRunning();
    return consumer;
  }

  static class Options {
    @CommandLine.Option(
        names = {"-h", "--help"},
        usageHelp = true,
        description = "display a help message")
    boolean helpRequested = false;

    @CommandLine.Option(names = "--service-url")
    String serviceUrl = "192.168.0.216:6570";

    @CommandLine.Option(names = "--stream-name-prefix")
    String streamNamePrefix = "read_bench_stream_";

    @CommandLine.Option(names = "--stream-replication-factor")
    short streamReplicationFactor = 1;

    @CommandLine.Option(names = "--stream-backlog-duration", description = "in seconds")
    int streamBacklogDuration = 60 * 30;

    @CommandLine.Option(names = "--record-size", description = "in bytes")
    int recordSize = 1024; // bytes

    @CommandLine.Option(names = "--total-write-size", description = "in bytes")
    long totalWriteSize = 30L * 1024 * 1024 * 1024; // bytes

    @CommandLine.Option(names = "--batch-size", description = "in bytes")
    int batchSize = 819200; // bytes

    @CommandLine.Option(names = "--ordering-keys")
    int orderingKeys = 10;

    @CommandLine.Option(names = "--bench-time", description = "in seconds")
    long benchmarkDuration = Integer.MAX_VALUE; // seconds

    @CommandLine.Option(names = "--warmup", description = "in seconds")
    long warm = 60; // seconds
  }
}
