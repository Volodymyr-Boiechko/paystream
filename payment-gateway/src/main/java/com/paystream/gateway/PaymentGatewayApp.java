package com.paystream.gateway;

import com.paystream.gateway.messaging.producer.PaymentProducer;
import com.paystream.gateway.service.PaymentGenerator;
import java.time.Instant;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PaymentGatewayApp {

    private static final int TOTAL_PAYMENTS = 100_000;

    private static final Logger log = LoggerFactory.getLogger(PaymentGatewayApp.class);

    public static void main(String[] args) {
        log.info("payment-gateway started at {} UTC", Instant.now());
        PaymentGenerator generator = new PaymentGenerator();

        try (PaymentProducer publisher = new PaymentProducer()) {
            IntStream.range(0, TOTAL_PAYMENTS)
                .forEach(index -> {
                    publisher.publish(generator.generate(index));
                    if (index % 100 == 0) {
                        log.info("queued {} payments", index);
                    }
                });
        }
    }
}
