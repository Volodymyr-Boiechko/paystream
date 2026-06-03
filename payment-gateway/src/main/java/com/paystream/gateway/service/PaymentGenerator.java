package com.paystream.gateway.service;

import com.paystream.gateway.dto.PaymentRequested;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class PaymentGenerator {

    private static final List<String> TRUSTED_COUNTRIES = List.of("US", "ES", "DE", "FR");
    private static final List<String> SUSPICIOUS_COUNTRIES = List.of("UA", "NG", "CN", "BR");

    public PaymentRequested generate(int index) {
        String accountId = "acc-" + (index % 100);
        int roll = ThreadLocalRandom.current().nextInt(100);

        String country;
        BigDecimal amount;

        if (roll < 80) {
            country = pick(TRUSTED_COUNTRIES);
            amount = normalAmount();
        } else if (roll < 95) {
            if (ThreadLocalRandom.current().nextBoolean()) {
                country = pick(SUSPICIOUS_COUNTRIES);
                amount = normalAmount();
            } else {
                country = pick(TRUSTED_COUNTRIES);
                amount = highAmount();
            }
        } else {
            country = pick(SUSPICIOUS_COUNTRIES);
            amount = highAmount();
        }

        return new PaymentRequested(
            "pay-" + index, accountId, amount, "EUR", country, Instant.now()
        );
    }

    private BigDecimal normalAmount() {
        int cents = ThreadLocalRandom.current().nextInt(0, 9_000);
        return BigDecimal.valueOf(cents, 2);
    }

    private BigDecimal highAmount() {
        int cents = ThreadLocalRandom.current().nextInt(9_001, 100_000);
        return BigDecimal.valueOf(cents, 2);
    }

    private String pick(List<String> list) {
        return list.get(ThreadLocalRandom.current().nextInt(list.size()));
    }
}
