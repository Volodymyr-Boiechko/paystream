package com.paystream.ledger.service;

import com.paystream.ledger.messaging.consumer.LedgerConsume;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class ConsumerManager {

    private final int consumerCount;
    private final List<LedgerConsume> consumers = new ArrayList<>();
    private final Supplier<LedgerConsume> ledgerConsumeSupplier;
    private ExecutorService executor;

    public ConsumerManager(int consumerCount, Supplier<LedgerConsume> ledgerConsumeSupplier) {
        this.consumerCount = consumerCount;
        this.ledgerConsumeSupplier = ledgerConsumeSupplier;
    }

    public void start() {
        executor = Executors.newFixedThreadPool(consumerCount);
        for (int i = 0; i < consumerCount; i++) {
            var consumer = ledgerConsumeSupplier.get();
            consumers.add(consumer);
            executor.submit(consumer::consume);
        }
    }

    public void stop() {
        consumers.forEach(LedgerConsume::stop);

        executor.shutdown();

        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        consumers.forEach(LedgerConsume::close);
    }
}
