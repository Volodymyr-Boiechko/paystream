package com.paystream.fraud.service;

import com.paystream.fraud.dto.PaymentRequested;
import java.math.BigDecimal;

public class NegativeAmountRule implements FraudRule {

    private static final int RISK = 100;

    @Override
    public int evaluate(PaymentRequested payment) {
        var amount = payment.amount();
        var lessThanZero = amount.compareTo(BigDecimal.ZERO) <= 0;
        return lessThanZero ? RISK : 0;
    }
}
