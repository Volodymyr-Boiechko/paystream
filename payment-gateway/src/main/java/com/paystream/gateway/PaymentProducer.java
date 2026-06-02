package com.paystream.gateway;

import com.paystream.gateway.config.AppProperties;
import com.paystream.gateway.dto.PaymentRequested;
import com.paystream.gateway.util.AppUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PaymentProducer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PaymentProducer.class);

    private static final String TOPIC = "payment.requested";

    private final KafkaProducer<String, String> producer;

    public PaymentProducer() {
        this.producer = new KafkaProducer<>(AppProperties.get());
    }

    public void publish(PaymentRequested payment) {
        String json = AppUtil.getJsonFromObject(payment);

        ProducerRecord<String, String> record = new ProducerRecord<>(
            TOPIC, payment.accountId(), json
        );

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("send failed for {}", payment.paymentId(), exception);
            }
        });
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }
}
