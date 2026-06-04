package com.paystream.ledger.config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseConfig {

    private final static String URL = "jdbc:postgresql://localhost:5432/ledger";
    private final static String USERNAME = "ledger";
    private final static String PASSWORD = "ledger";

    public static Connection getConnection() {
        try {
            return DriverManager.getConnection(URL, USERNAME, PASSWORD);
        } catch (SQLException exception) {
            throw new RuntimeException(exception);
        }
    }
}
