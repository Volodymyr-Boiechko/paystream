package com.paystream.ledger.service;

import com.paystream.ledger.messaging.consumer.PaymentAuthorizedConsumer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConsumerManager {

    private final int consumerCount;
    private final List<PaymentAuthorizedConsumer> consumers = new ArrayList<>();
    private ExecutorService executor;

    public ConsumerManager(int consumerCount) {
        this.consumerCount = consumerCount;
    }

    public void start() {
        executor = Executors.newFixedThreadPool(consumerCount);
        for (int i = 0; i < consumerCount; i++) {
            var consumer = new PaymentAuthorizedConsumer();
            consumers.add(consumer);
            executor.submit(consumer::consume);
        }
    }

    public void stop() {
        consumers.forEach(PaymentAuthorizedConsumer::stop);

        executor.shutdown();

        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        consumers.forEach(PaymentAuthorizedConsumer::close);
    }
}
