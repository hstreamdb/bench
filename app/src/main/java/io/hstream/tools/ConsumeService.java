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
  private final List<Subscription> subs;
  private final HStreamClient client;

  public ConsumeService(
      HStreamClient client,
      List<String> streams,
      int ackTimeout,
      AtomicBoolean warmupDone,
      Stats stat,
      int payloadSize,
      String offset) {
    this.consumers = new ArrayList<>(streams.size());
    this.subs = new ArrayList<>(streams.size());
    this.client = client;

    for (String stream : streams) {
      consumers.add(createConsumer(stream, ackTimeout, warmupDone, stat, payloadSize, offset));
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
    for (var sub : subs) {
      client.deleteSubscription(sub.getSubscriptionId(), true);
    }
  }

  private Consumer createConsumer(
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

    var sub =
        Subscription.newBuilder().stream(streamName)
            .subscription(subscriptionId)
            .ackTimeoutSeconds(ackTimeout)
            .offset(subOffset)
            .build();
    client.createSubscription(sub);
    subs.add(sub);
    System.out.println("created subscription: " + subscriptionId);
    return client
        .newConsumer()
        .subscription(subscriptionId)
        .rawRecordReceiver(
            (receivedRawRecord, responder) -> {
              if (warmupDone.get()) {
                Instant sendTime = receivedRawRecord.getCreatedTime();
                Instant currTime = Instant.now();
                long latencyMicros =
                    TimeUnit.NANOSECONDS.toMicros(
                        Utils.instantToNano(currTime) - Utils.instantToNano(sendTime));
                stats.recordMessageReceived(payloadSize, latencyMicros);
              }
              responder.ack();
            })
        .hRecordReceiver(
            (receivedHRecord, responder) -> {
              if (warmupDone.get()) {
                Instant sendTime = receivedHRecord.getCreatedTime();
                Instant currTime = Instant.now();
                long latencyMicros =
                    TimeUnit.NANOSECONDS.toMicros(
                        Utils.instantToNano(currTime) - Utils.instantToNano(sendTime));
                stats.recordMessageReceived(payloadSize, latencyMicros);
              }
              responder.ack();
            })
        .build();
  }
}
