package com.paystream.fraud.service;

import com.paystream.fraud.dto.PaymentRequested;
import java.math.BigDecimal;

public class HighAmountRule implements FraudRule {

    private static final int RISK = 40;
    private static final BigDecimal HIGH_AMOUNT = new BigDecimal("90");

    @Override
    public int evaluate(PaymentRequested payment) {
        var amount = payment.amount();
        var moreThanValue = amount.compareTo(HIGH_AMOUNT) > 0;
        return moreThanValue ? RISK : 0;
    }
}
