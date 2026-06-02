package com.paystream.gateway;

import com.paystream.gateway.dto.PaymentRequested;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

public class PaymentGenerator {

    public PaymentRequested generate(int index) {
        String accountId = "acc-" + (index % 100);

        return new PaymentRequested(
            "pay-" + index, accountId, generateRandomAmount(),
            "EUR", "US", Instant.now()
        );
    }

    private BigDecimal generateRandomAmount() {
        int cents = ThreadLocalRandom.current().nextInt(0, 10_000);
        return BigDecimal.valueOf(cents, 2);
    }
}
