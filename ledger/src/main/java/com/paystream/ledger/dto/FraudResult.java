package com.paystream.ledger.dto;

import java.time.Instant;

public record FraudResult(
    PaymentRequested payment,
    PaymentStatus status,
    Integer riskScore,
    Instant reviewedAt
) {
}
