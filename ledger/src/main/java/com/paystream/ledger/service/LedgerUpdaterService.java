package com.paystream.ledger.service;

import com.paystream.ledger.dto.FraudResult;
import com.paystream.ledger.dto.LedgerEntry;
import com.paystream.ledger.messaging.producer.LedgerUpdatedProducer;
import com.paystream.ledger.repo.TransactionRepository;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LedgerUpdaterService {

    private static final Logger log = LoggerFactory.getLogger(LedgerUpdaterService.class);

    private final TransactionRepository transactionRepository;
    private final LedgerUpdatedProducer ledgerUpdatedProducer;

    public LedgerUpdaterService() {
        this.transactionRepository = new TransactionRepository();
        this.ledgerUpdatedProducer = new LedgerUpdatedProducer();
    }

    public void process(FraudResult fraudResult) {

        var paymentId = fraudResult.payment().paymentId();

        var isNew = transactionRepository.saveTransaction(fraudResult);

        if (!isNew) {
            log.warn("Transaction with id {} already exists in ledger", paymentId);
            return;
        }

        var accountId = fraudResult.payment().accountId();
        var accountBalance = transactionRepository.getBalanceOfTheAccount(accountId);

        var ledgerEntry = new LedgerEntry(
            paymentId, accountId, fraudResult.payment().amount(), accountBalance, Instant.now()
        );

        ledgerUpdatedProducer.sendLedgerEntry(ledgerEntry);
    }
}
