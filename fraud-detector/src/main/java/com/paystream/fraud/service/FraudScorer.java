package com.paystream.fraud.service;

import static com.paystream.fraud.dto.PaymentStatus.APPROVED;
import static com.paystream.fraud.dto.PaymentStatus.FLAGGED;

import com.paystream.fraud.dto.FraudResult;
import com.paystream.fraud.dto.PaymentRequested;
import java.time.Instant;
import java.util.List;

public class FraudScorer {

    private final List<FraudRule> rules;

    public FraudScorer(List<FraudRule> rules) {
        this.rules = rules;
    }

    public FraudResult score(PaymentRequested request) {
        int totalRisk = rules.stream()
            .mapToInt(rule -> rule.evaluate(request))
            .sum();

        var decision = totalRisk >= 50 ? FLAGGED : APPROVED;
        return new FraudResult(request, decision, totalRisk, Instant.now());
    }
}
