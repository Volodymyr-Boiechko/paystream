package com.paystream.fraud.service;

import com.paystream.fraud.dto.PaymentRequested;

public interface FraudRule {

    int evaluate(PaymentRequested payment);
}
