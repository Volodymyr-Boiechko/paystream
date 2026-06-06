package com.paystream.notification;

import com.paystream.notification.service.ConsumerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotificationApp {

    private static final Logger log = LoggerFactory.getLogger(NotificationApp.class);

    private static final int NUMBER_OF_CONSUMERS = 2;

    public static void main(String[] args) {
        log.info("notification started");

        ConsumerManager consumerManager = new ConsumerManager(NUMBER_OF_CONSUMERS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutdown requested, stopping consumer...");
            consumerManager.stop();
        }));

        consumerManager.start();

    }
}
