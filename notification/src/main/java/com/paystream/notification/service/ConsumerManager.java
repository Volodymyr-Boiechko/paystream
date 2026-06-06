package com.paystream.notification.service;

import com.paystream.notification.messaging.consumer.LedgerEntryConsumer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConsumerManager {

    private final int consumerCount;
    private final List<LedgerEntryConsumer> consumers = new ArrayList<>();
    private ExecutorService executor;

    public ConsumerManager(int consumersCount) {
        this.consumerCount = consumersCount;
    }

    public void start() {
        executor = Executors.newFixedThreadPool(consumerCount);
        for (int i = 0; i < consumerCount; i++) {
            var consumer = new LedgerEntryConsumer();
            consumers.add(consumer);
            executor.submit(consumer::consume);
        }
    }


    public void stop() {
        consumers.forEach(LedgerEntryConsumer::stop);

        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        consumers.forEach(LedgerEntryConsumer::close);
    }
}
