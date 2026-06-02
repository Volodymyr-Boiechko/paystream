package com.paystream.gateway.dto;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentRequested(
    String paymentId,
    String accountId,
    BigDecimal amount,
    String currency,
    String country,
    Instant createdAt
) {
}
