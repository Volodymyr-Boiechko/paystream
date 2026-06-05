package com.paystream.ledger.repo;

import com.paystream.ledger.config.DatabaseConfig;
import com.paystream.ledger.dto.FraudResult;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionRepository {

    private static final Logger logger = LoggerFactory.getLogger(TransactionRepository.class);

    public boolean saveTransaction(FraudResult result) {

        final String query = """
                INSERT INTO transactions
                (
                    payment_id, account_id, amount, currency, country, status,
                    risk_score, created_at, reviewed_at, recorded_at
                )
                VALUES
                (
                    ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?
                )
                ON CONFLICT (payment_id) DO NOTHING 
            """;

        try (Connection connection = DatabaseConfig.getConnection();
            PreparedStatement statement = connection.prepareStatement(query)) {

            statement.setString(1, result.payment().paymentId());
            statement.setString(2, result.payment().accountId());
            statement.setBigDecimal(3, result.payment().amount());
            statement.setString(4, result.payment().currency());
            statement.setString(5, result.payment().country());
            statement.setString(6, result.status().name());
            statement.setInt(7, result.riskScore());
            statement.setObject(8, result.payment().createdAt().atOffset(ZoneOffset.UTC));
            statement.setObject(9, result.reviewedAt().atOffset(ZoneOffset.UTC));
            statement.setObject(10, Instant.now().atOffset(ZoneOffset.UTC));

            return statement.executeUpdate() > 0;

        } catch (SQLException e) {
            logger.error("Exception occurred while saving transaction", e);
            throw new RuntimeException("DB error saving transaction", e);
        }
    }

    public BigDecimal getBalanceOfTheAccount(String accountId) {

        final String query = """
                SELECT COALESCE(SUM(t.amount), 0)
                FROM transactions t
                WHERE t.account_id = ?
            """;

        try (Connection connection = DatabaseConfig.getConnection();
            PreparedStatement statement = connection.prepareStatement(query)) {

            statement.setString(1, accountId);

            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? rs.getBigDecimal(1) : BigDecimal.ZERO;
            }

        } catch (SQLException e) {
            logger.error("Exception occurred while getting balance of the account", e);
            throw new RuntimeException(e);
        }
    }
}
