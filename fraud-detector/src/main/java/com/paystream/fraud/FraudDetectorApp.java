package com.paystream.fraud;

import com.paystream.fraud.messaging.consumer.PaymentConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FraudDetectorApp {

    private static final Logger log = LoggerFactory.getLogger(FraudDetectorApp.class);

    public static void main(String[] args) {
        log.info("fraud-detector started");

        PaymentConsumer consumer = new PaymentConsumer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutdown requested, stopping consumer...");
            consumer.stop();
        }));

        try (consumer) {
            consumer.consume();
        }
    }
}
