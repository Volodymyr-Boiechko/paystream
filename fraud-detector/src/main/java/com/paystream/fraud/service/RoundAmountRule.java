package com.paystream.fraud.service;

import com.paystream.fraud.dto.PaymentRequested;
import java.math.BigDecimal;

public class RoundAmountRule implements FraudRule {

    private static final int RISK = 15;

    @Override
    public int evaluate(PaymentRequested payment) {
        var amount = payment.amount();
        boolean isRound = amount.remainder(BigDecimal.ONE).compareTo(BigDecimal.ZERO) == 0;
        return isRound ? RISK : 0;
    }
}
