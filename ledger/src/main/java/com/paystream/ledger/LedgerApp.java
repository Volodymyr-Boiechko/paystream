package com.paystream.ledger;

import com.paystream.ledger.messaging.consumer.PaymentAuthorizedConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LedgerApp {

    private static final Logger log = LoggerFactory.getLogger(LedgerApp.class);

    public static void main(String[] args) {
        log.info("ledger started");

        PaymentAuthorizedConsumer consumer = new PaymentAuthorizedConsumer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutdown requested, stopping consumer...");
            consumer.stop();
        }));

        try (consumer){
            consumer.consume();
        }
    }
}
