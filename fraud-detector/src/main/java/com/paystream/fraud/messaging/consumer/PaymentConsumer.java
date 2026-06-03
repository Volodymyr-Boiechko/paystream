package com.paystream.fraud.messaging.consumer;

import com.paystream.fraud.config.AppConfig;
import com.paystream.fraud.config.MessagingConfig;
import com.paystream.fraud.dto.PaymentRequested;
import com.paystream.fraud.dto.PaymentStatus;
import com.paystream.fraud.messaging.producer.PaymentProducer;
import com.paystream.fraud.service.FraudScorer;
import com.paystream.fraud.util.AppUtil;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PaymentConsumer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PaymentConsumer.class);

    private static final String CONSUMER_TOPIC = "payment.requested";
    private static final String AUTHORIZED_TOPIC = "payment.authorized";
    private static final String FLAGGED_TOPIC = "payment.flagged";

    private final FraudScorer fraudScorer;
    private final KafkaConsumer<String, String> consumer;
    private final PaymentProducer producer;

    public PaymentConsumer() {
        this.consumer = new KafkaConsumer<>(MessagingConfig.getConsumerProperties());
        this.producer = new PaymentProducer();
        this.fraudScorer = AppConfig.fraudScorer();
    }

    public void consume() {
        consumer.subscribe(List.of(CONSUMER_TOPIC));

        int count = 0;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                var payload = record.value();
                var paymentRequested = AppUtil.readFromJson(payload, PaymentRequested.class);

                var fraudScore = fraudScorer.score(paymentRequested);
                var topic = fraudScore.status() == PaymentStatus.APPROVED ? AUTHORIZED_TOPIC : FLAGGED_TOPIC;

                producer.produce(topic, fraudScore);

                count++;
                if (count % 1000 == 0) {
                    log.info("Payment requested: {}", paymentRequested);
                    log.info("processed {}, partition {} offset {}", count, record.partition(), record.offset());
                }
            }

            commitProcessed();
        }
    }

    private void commitProcessed() {
        try {
            consumer.commitSync();
        } catch (CommitFailedException e) {
            log.warn("commit failed, partition reassigned: {}", e.getMessage());
        } catch (KafkaException e) {
            log.error("commit error, will retry on next batch", e);
        }
    }

    @Override
    public void close() {
        consumer.close();
    }
}
