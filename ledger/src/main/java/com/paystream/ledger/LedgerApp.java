package com.paystream.ledger;

import com.paystream.ledger.messaging.consumer.PaymentAuthorizedConsumer;
import com.paystream.ledger.messaging.consumer.PaymentRetryConsumer;
import com.paystream.ledger.service.ConsumerManager;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LedgerApp {

    private static final Logger log = LoggerFactory.getLogger(LedgerApp.class);

    public static void main(String[] args) {
        log.info("ledger started");

        getManagers()
            .forEach(manager -> {
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    log.warn("shutdown requested, stopping consumer...");
                    manager.stop();
                }));

                manager.start();
            });
    }

    private static List<ConsumerManager> getManagers() {
        return List.of(
            new ConsumerManager(3, PaymentAuthorizedConsumer::new),
            new ConsumerManager(1, PaymentRetryConsumer::new)
        );
    }
}
