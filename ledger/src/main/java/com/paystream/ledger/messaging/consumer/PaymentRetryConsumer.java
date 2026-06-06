package com.paystream.ledger.messaging.consumer;

public class PaymentRetryConsumer extends AbstractLedgerConsumer {

    private static final String AUTHORIZED_TOPIC = "payment.authorized.retry";

    public PaymentRetryConsumer() {
        super(AUTHORIZED_TOPIC, "ledger-retry");
    }
}
