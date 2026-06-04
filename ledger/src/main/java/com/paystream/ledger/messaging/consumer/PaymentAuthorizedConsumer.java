package com.paystream.ledger.messaging.consumer;

import com.paystream.ledger.config.MessagingConfig;
import com.paystream.ledger.dto.FraudResult;
import com.paystream.ledger.dto.LedgerEntry;
import com.paystream.ledger.messaging.producer.LedgerUpdatedProducer;
import com.paystream.ledger.repo.TransactionRepository;
import com.paystream.ledger.util.AppUtil;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PaymentAuthorizedConsumer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PaymentAuthorizedConsumer.class);

    private volatile boolean running = true;

    private static final String AUTHORIZED_TOPIC = "payment.authorized";

    private final KafkaConsumer<String, String> consumer;
    private final LedgerUpdatedProducer ledgerUpdatedProducer;
    private final TransactionRepository transactionRepository;

    public PaymentAuthorizedConsumer() {
        this.consumer = new KafkaConsumer<>(MessagingConfig.getConsumerProperties());
        this.ledgerUpdatedProducer = new LedgerUpdatedProducer();
        this.transactionRepository = new TransactionRepository();
    }

    public void consume() {
        consumer.subscribe(List.of(AUTHORIZED_TOPIC));

        int count = 0;

        while (running) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                var payload = record.value();
                var fraudResult = AppUtil.readFromJson(payload, FraudResult.class);

                var paymentId = fraudResult.payment().paymentId();

                var isNew = transactionRepository.saveTransaction(fraudResult);

                if (!isNew) {
                    log.info("Transaction with id {} already exists in ledger", paymentId);
                    continue;
                }

                var accountId = fraudResult.payment().accountId();
                var accountBalance = transactionRepository.getBalanceOfTheAccount(accountId);

                var ledgerEntry = new LedgerEntry(
                    paymentId, accountId, fraudResult.payment().amount(), accountBalance, Instant.now()
                );

                ledgerUpdatedProducer.sendLedgerEntry(ledgerEntry);

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
