package com.paystream.notification.messaging.consumer;

import com.paystream.notification.config.MessagingConfig;
import com.paystream.notification.dto.LedgerEntry;
import com.paystream.notification.service.NotificationSender;
import com.paystream.notification.util.AppUtil;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LedgerEntryConsumer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LedgerEntryConsumer.class);

    private static final String LEDGER_TOPIC = "ledger.updated";

    private final KafkaConsumer<String, String> consumer;
    private final NotificationSender notificationSender;
    private volatile boolean running = true;

    public LedgerEntryConsumer() {
        this.consumer = new KafkaConsumer<>(MessagingConfig.getConsumerProperties());
        this.notificationSender = new NotificationSender();
    }

    public void consume() {

        int count = 0;

        consumer.subscribe(List.of(LEDGER_TOPIC));

        while (running) {

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {

                var value = record.value();

                var ledgerEntry = AppUtil.readFromJson(value, LedgerEntry.class);

                notificationSender.sendNotification(ledgerEntry);

                count++;
                if (count % 1000 == 0) {
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

    public void stop() {
        running = false;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
