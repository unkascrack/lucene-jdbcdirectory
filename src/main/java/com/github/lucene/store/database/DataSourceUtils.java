package com.github.lucene.store.database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

class DataSourceUtils {

    private DataSourceUtils() {
    }

    static Connection getConnection(final DataSource dataSource) throws DatabaseStoreException {
        try {
            return dataSource.getConnection();
        } catch (final SQLException e) {
            throw new DatabaseStoreException(e);
        }
    }

    static void releaseConnection(final Connection connection) throws DatabaseStoreException {
        if (connection != null) {
            try {
                connection.close();
            } catch (final SQLException e) {
                throw new DatabaseStoreException(e);
            }
        }
    }

    /**
     * Close the given JDBC Statement and ignore any thrown exception. This is
     * useful for typical finally blocks in manual JDBC code.
     *
     * @param statement
     *            the JDBC Statement to close
     */
    public static void closeStatement(final Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (final SQLException e) {
            }
        }
    }

    /**
     * Close the given JDBC ResultSet and ignore any thrown exception. This is
     * useful for typical finally blocks in manual JDBC code.
     *
     * @param resultSet
     *            the JDBC ResultSet to close
     */
    public static void closeResultSet(final ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (final SQLException e) {
            }
        }
    }
}
