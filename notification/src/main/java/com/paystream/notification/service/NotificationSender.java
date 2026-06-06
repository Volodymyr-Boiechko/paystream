package com.paystream.notification.service;

import com.paystream.notification.dto.LedgerEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotificationSender {

    private static final Logger log = LoggerFactory.getLogger(NotificationSender.class);

    public void sendNotification(LedgerEntry ledgerEntry) {
        log.info("Sending ledger entry: {}", ledgerEntry);
    }
}
