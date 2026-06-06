package com.paystream.ledger.messaging.consumer;

public interface LedgerConsume {

    void consume();

    void stop();

    void close();
}
