package com.paystream.fraud.messaging.producer;

import com.paystream.fraud.config.MessagingConfig;
import com.paystream.fraud.dto.FraudResult;
import com.paystream.fraud.util.AppUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PaymentProducer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PaymentProducer.class);

    private final KafkaProducer<String, String> producer;

    public PaymentProducer() {
        this.producer = new KafkaProducer<>(MessagingConfig.getProducerProperties());
    }

    public void produce(String topic, FraudResult result) {
        String json = AppUtil.getJsonFromObject(result);
        String key = result.payment().accountId();

        ProducerRecord<String, String> record = new ProducerRecord<>(
            topic, key, json
        );

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("send failed for {}", result.payment().paymentId(), exception);
            }
        });

    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }
}
