package com.paystream.ledger.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class DatabaseConfig {

    private static final String URL = env("DB_URL", "jdbc:postgresql://localhost:5432/ledger");
    private static final String USERNAME = env("DB_USERNAME", "ledger");
    private static final String PASSWORD = env("DB_PASSWORD", "ledger");

    private static final HikariDataSource DATA_SOURCE = createDataSource();

    private DatabaseConfig() {
    }

    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isBlank()) ? value : defaultValue;
    }

    private static HikariDataSource createDataSource() {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName("org.postgresql.Driver");
        config.setJdbcUrl(URL);
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);
        config.setMaximumPoolSize(10);
        return new HikariDataSource(config);
    }

    public static Connection getConnection() {
        try {
            return DATA_SOURCE.getConnection();
        } catch (SQLException exception) {
            throw new RuntimeException(exception);
        }
    }
}
