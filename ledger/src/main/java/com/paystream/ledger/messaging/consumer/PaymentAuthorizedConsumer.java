package com.paystream.ledger.messaging.consumer;

public class PaymentAuthorizedConsumer extends AbstractLedgerConsumer {

    private static final String AUTHORIZED_TOPIC = "payment.authorized";

    public PaymentAuthorizedConsumer() {
        super(AUTHORIZED_TOPIC, "ledger");
    }
}
