package io.hstream.tools;

import io.hstream.Consumer;
import io.hstream.HStreamClient;
import io.hstream.Subscription;
import io.hstream.tools.Stats.Stats;
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
      int payloadSize) {
    this.consumers = new ArrayList<>(streams.size());

    for (String stream : streams) {
      consumers.add(createConsumer(client, stream, ackTimeout, warmupDone, stat, payloadSize));
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
      int payloadSize) {
    var subscriptionId = "sub_" + UUID.randomUUID();
    client.createSubscription(
        Subscription.newBuilder().stream(streamName)
            .subscription(subscriptionId)
            .ackTimeoutSeconds(ackTimeout)
            .offset(Subscription.SubscriptionOffset.EARLIEST)
            .build());
    return client
        .newConsumer()
        .subscription(subscriptionId)
        .rawRecordReceiver(
            (receivedRawRecord, responder) -> {
              if (warmupDone.get()) {
                long sendTime = receivedRawRecord.getCreatedTime().toEpochMilli();
                long currTime = System.currentTimeMillis();
                long latencyMicros = TimeUnit.MILLISECONDS.toMicros(currTime - sendTime);
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
