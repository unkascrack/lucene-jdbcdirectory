package com.github.lucene.store.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JdbcTemplate {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcTemplate.class);

    /**
     * A callback interface used to initialize a Jdbc
     * <code>PreparedStatement</code>.
     */
    static interface PrepateStatementAwareCallback {

        /**
         * Initialize/Fill the given <code>PreparedStatement</code>.
         */
        void fillPrepareStatement(PreparedStatement ps) throws Exception;
    }

    /**
     * A callback used to retrieve data from a <code>ResultSet</code>.
     */
    static interface ExecuteSelectCallback extends PrepateStatementAwareCallback {

        /**
         * Extract data from the <code>ResultSet</code> and an optional return
         * value.
         */
        Object execute(ResultSet rs) throws Exception;
    }

    private JdbcTemplate() {
    }

    /**
     * A template method to execute a simple sql select statement. The jdbc
     * <code>Connection</code>, <code>PreparedStatement</code>, and
     * <code>ResultSet</code> are managed by the template.
     *
     * @param connection
     * @param sql
     * @param callback
     * @return
     * @throws DatabaseStoreException
     */
    static Object executeSelect(final Connection connection, final String sql, final ExecuteSelectCallback callback)
            throws DatabaseStoreException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = connection.prepareStatement(sql);
            callback.fillPrepareStatement(ps);
            rs = ps.executeQuery();
            return callback.execute(rs);
        } catch (final Exception e) {
            LOGGER.warn("DatabaseDirectory: failed to execute sql [{}]: e", sql, e.getMessage());
            throw (DatabaseStoreException) (e instanceof DatabaseStoreException ? e
                    : new DatabaseStoreException("Failed to execute sql [" + sql + "]", e));
        } finally {
            DataSourceUtils.closeResultSet(rs);
            DataSourceUtils.closeStatement(ps);
            DataSourceUtils.releaseConnection(connection);
        }
    }

    /**
     * A template method to execute a simple sql update. The jdbc
     * <code>Connection</code>, and <code>PreparedStatement</code> are managed
     * by the template. A <code>PreparedStatement</code> can be used to set
     * values to the given sql.
     *
     * @param connection
     * @param sql
     * @param callback
     * @throws DatabaseStoreException
     */
    static void executeUpdate(final Connection connection, final String sql,
            final PrepateStatementAwareCallback callback) throws DatabaseStoreException {
        executeUpdate(connection, sql, false, callback);
    }

    /**
     * A template method to execute a simple sql update. The jdbc
     * <code>Connection</code>, and <code>PreparedStatement</code> are managed
     * by the template. A <code>PreparedStatement</code> can be used to set
     * values to the given sql.
     *
     * @param connection
     * @param sql
     * @param commit
     * @param callback
     * @throws DatabaseStoreException
     */
    static void executeUpdate(final Connection connection, final String sql, final boolean commit,
            final PrepateStatementAwareCallback callback) throws DatabaseStoreException {
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(sql);
            callback.fillPrepareStatement(ps);
            ps.executeUpdate();
            if (commit) {
                connection.commit();
            }
        } catch (final Exception e) {
            LOGGER.warn("DatabaseDirectory: failed to execute sql [{}]: e", sql, e.getMessage());
            throw (DatabaseStoreException) (e instanceof DatabaseStoreException ? e
                    : new DatabaseStoreException("Failed to execute sql [" + sql + "]", e));
        } finally {
            DataSourceUtils.closeStatement(ps);
            DataSourceUtils.releaseConnection(connection);
        }
    }
}
