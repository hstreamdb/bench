package io.hstream.tools;

import io.hstream.Consumer;
import io.hstream.HStreamClient;
import io.hstream.Subscription;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ConsumeService {
  private final List<Consumer> consumers;

  public ConsumeService(
      HStreamClient client,
      List<String> streams,
      int ackTimeout,
      AtomicBoolean warmupDone,
      AtomicLong successReads) {
    this.consumers = new ArrayList<>(streams.size());

    for (String stream : streams) {
      consumers.add(createConsumer(client, stream, ackTimeout, warmupDone, successReads));
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
      AtomicLong successReads) {
    var subscriptionId = UUID.randomUUID().toString();
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
                successReads.incrementAndGet();
              }
              responder.ack();
            })
        .hRecordReceiver(
            (receivedHRecord, responder) -> {
              if (warmupDone.get()) {
                successReads.incrementAndGet();
              }
              responder.ack();
            })
        .build();
  }
}
