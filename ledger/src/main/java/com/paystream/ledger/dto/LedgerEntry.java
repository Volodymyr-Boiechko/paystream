package com.paystream.ledger.dto;

import java.math.BigDecimal;
import java.time.Instant;

public record LedgerEntry(
    String paymentId,
    String accountId,
    BigDecimal amount,
    BigDecimal balanceAfter,
    Instant updatedAt
) {
}
