package com.paystream.fraud.service;

import com.paystream.fraud.dto.PaymentRequested;
import java.util.Set;

public class SuspiciousCountryRule implements FraudRule {

    private static final Set<String> TRUSTED = Set.of("US", "ES", "DE", "FR");
    private static final int RISK = 30;

    @Override
    public int evaluate(PaymentRequested payment) {
        var country = payment.country();
        return TRUSTED.contains(country) ? 0 : RISK;
    }
}
