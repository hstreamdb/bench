package io.hstream.tools;

import io.hstream.Consumer;
import io.hstream.HStreamClient;
import io.hstream.Subscription;
import io.hstream.tools.Stats.Stats;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumeService {
  private final List<Consumer> consumers;

  public ConsumeService(
      HStreamClient client,
      List<String> streams,
      int ackTimeout,
      AtomicBoolean warmupDone,
      Stats stat,
      int payloadSize,
      String offset) {
    this.consumers = new ArrayList<>(streams.size());

    for (String stream : streams) {
      consumers.add(createConsumer(client, stream, ackTimeout, warmupDone, stat, payloadSize, offset));
    }
  }

  public void startConsume() {
    for (var consumer : consumers) {
      consumer.startAsync().awaitRunning();
    }
  }

  public void stopConsume() {
    for (var consumer : consumers) {
      consumer.stopAsync().awaitTerminated();
    }
  }

  private Consumer createConsumer(
      HStreamClient client,
      String streamName,
      int ackTimeout,
      AtomicBoolean warmupDone,
      Stats stats,
      int payloadSize,
      String offset) {
    var subscriptionId = "sub_" + UUID.randomUUID();
    Subscription.SubscriptionOffset subOffset;
    if (offset.equals("latest")) {
      subOffset = Subscription.SubscriptionOffset.LATEST;
    } else if (offset.equals("earliest")) {
      subOffset = Subscription.SubscriptionOffset.EARLIEST;
    } else {
      throw new RuntimeException("invalied sub offset: " + offset);
    }

    client.createSubscription(
        Subscription.newBuilder().stream(streamName)
            .subscription(subscriptionId)
            .ackTimeoutSeconds(ackTimeout)
            .offset(subOffset)
            .build());
    return client
        .newConsumer()
        .subscription(subscriptionId)
        .rawRecordReceiver(
            (receivedRawRecord, responder) -> {
              if (warmupDone.get()) {
                Instant sendTime = receivedRawRecord.getCreatedTime();
                Instant currTime = Instant.now();
//                long currTime = System.nanoTime();
                long diffMils = currTime.toEpochMilli() - sendTime.toEpochMilli();
//                long latencyMicros = TimeUnit.MILLISECONDS.toMicros(currTime - sendTime);
                long latencyMicros = TimeUnit.MILLISECONDS.toMicros(diffMils);
//                System.out.printf("latencyMicros: %d\n", latencyMicros);
                stats.recordMessageReceived(payloadSize, latencyMicros);
              }
              responder.ack();
            })
        .hRecordReceiver(
            (receivedHRecord, responder) -> {
              if (warmupDone.get()) {
                long sendTime = receivedHRecord.getCreatedTime().toEpochMilli();
                long currTime = System.currentTimeMillis();
                long latencyMicros = TimeUnit.MILLISECONDS.toMicros(currTime - sendTime);
                stats.recordMessageReceived(payloadSize, latencyMicros);
              }
              responder.ack();
            })
        .build();
  }
}
