/*
 * Copyright 2004-2009 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.lucene.store.jdbc.support;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lucene.store.jdbc.JdbcStoreException;
import com.github.lucene.store.jdbc.datasource.DataSourceUtils;

/**
 * Helper class that isused to encapsulate resource and transaction handling
 * related to <code>DataSource</code>, <code>Statement</code>, and
 * <code>ResultSet</code>. {@link DataSourceUtils} is used to open/cose relevant
 * resources.
 *
 * @author kimchy
 * @see DataSourceUtils
 */
public class JdbcTemplate {

    private static final Logger log = LoggerFactory.getLogger(JdbcTemplate.class);

    /**
     * A callback interface used to initialize a Jdbc
     * <code>PreparedStatement</code>.
     */
    public static interface PrepateStatementAwareCallback {

        /**
         * Initialize/Fill the given <code>PreparedStatement</code>.
         */
        void fillPrepareStatement(PreparedStatement ps) throws Exception;
    }

    /**
     * A callback used to retrieve data from a <code>ResultSet</code>.
     */
    public static interface ExecuteSelectCallback extends PrepateStatementAwareCallback {

        /**
         * Extract data from the <code>ResultSet</code> and an optional return
         * value.
         */
        Object execute(ResultSet rs) throws Exception;
    }

    /**
     * A callback used to work with <code>CallableStatement</code>.
     */
    public static interface CallableStatementCallback {

        /**
         * initialize/Fill the <code>CallableStatement</code> before it is
         * executed.
         */
        void fillCallableStatement(CallableStatement cs) throws Exception;

        /**
         * Read/Extract data from the result of the
         * <code>CallableStatement</code> execution. CAn optionally have a
         * return value.
         */
        Object readCallableData(CallableStatement cs) throws Exception;
    }

    private final DataSource dataSource;

    /**
     * Creates a new <code>JdbcTemplate</code>.
     */
    public JdbcTemplate(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * A template method to execute a simple sql select statement. The jdbc
     * <code>Connection</code>, <code>PreparedStatement</code>, and
     * <code>ResultSet</code> are managed by the template.
     */
    public Object executeSelect(final String sql, final ExecuteSelectCallback callback) throws JdbcStoreException {
        final Connection con = DataSourceUtils.getConnection(dataSource);
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = con.prepareStatement(sql);
            // ps.setQueryTimeout(settings.getQueryTimeout());
            callback.fillPrepareStatement(ps);
            rs = ps.executeQuery();
            return callback.execute(rs);
        } catch (final JdbcStoreException e) {
            if (log.isTraceEnabled()) {
                log.trace("Failed to execute sql [" + sql + "]", e);
            }
            throw e;
        } catch (final Exception e) {
            if (log.isTraceEnabled()) {
                log.trace("Failed to execute sql [" + sql + "]", e);
            }
            throw new JdbcStoreException("Failed to execute sql [" + sql + "]", e);
        } finally {
            DataSourceUtils.closeResultSet(rs);
            DataSourceUtils.closeStatement(ps);
            DataSourceUtils.releaseConnection(con);
        }
    }

    /**
     * A template method to execute a simple sql callable statement. The jdbc
     * <code>Connection</code>, and <code>CallableStatement</code> are managed
     * by the template.
     */
    public Object executeCallable(final String sql, final CallableStatementCallback callback)
            throws JdbcStoreException {
        final Connection con = DataSourceUtils.getConnection(dataSource);
        CallableStatement cs = null;
        try {
            cs = con.prepareCall(sql);
            // cs.setQueryTimeout(settings.getQueryTimeout());
            callback.fillCallableStatement(cs);
            cs.execute();
            return callback.readCallableData(cs);
        } catch (final JdbcStoreException e) {
            if (log.isTraceEnabled()) {
                log.trace("Failed to execute sql [" + sql + "]", e);
            }
            throw e;
        } catch (final Exception e) {
            if (log.isTraceEnabled()) {
                log.trace("Failed to execute sql [" + sql + "]", e);
            }
            throw new JdbcStoreException("Failed to execute sql [" + sql + "]", e);
        } finally {
            DataSourceUtils.closeStatement(cs);
            DataSourceUtils.releaseConnection(con);
        }
    }

    /**
     * A template method to execute a simple sql update. The jdbc
     * <code>Connection</code>, and <code>PreparedStatement</code> are managed
     * by the template. A <code>PreparedStatement</code> can be used to set
     * values to the given sql.
     */
    public void executeUpdate(final String sql, final PrepateStatementAwareCallback callback)
            throws JdbcStoreException {
        final Connection con = DataSourceUtils.getConnection(dataSource);
        PreparedStatement ps = null;
        try {
            ps = con.prepareStatement(sql);
            // ps.setQueryTimeout(settings.getQueryTimeout());
            callback.fillPrepareStatement(ps);
            ps.executeUpdate();
        } catch (final JdbcStoreException e) {
            if (log.isTraceEnabled()) {
                log.trace("Failed to execute sql [" + sql + "]", e);
            }
            throw e;
        } catch (final Exception e) {
            if (log.isTraceEnabled()) {
                log.trace("Failed to execute sql [" + sql + "]", e);
            }
            throw new JdbcStoreException("Failed to execute sql [" + sql + "]", e);
        } finally {
            DataSourceUtils.closeStatement(ps);
            DataSourceUtils.releaseConnection(con);
        }
    }

    /**
     * A template method to execute a simpel sql update (with no need for data
     * initialization).
     */
    public void executeUpdate(final String sql) throws JdbcStoreException {
        final Connection con = DataSourceUtils.getConnection(dataSource);
        Statement statement = null;
        try {
            statement = con.createStatement();
            // statement.setQueryTimeout(settings.getQueryTimeout());
            statement.executeUpdate(sql);
        } catch (final SQLException e) {
            if (log.isTraceEnabled()) {
                log.trace("Failed to execute sql [" + sql + "]", e);
            }
            throw new JdbcStoreException("Failed to execute [" + sql + "]", e);
        } finally {
            DataSourceUtils.closeStatement(statement);
            DataSourceUtils.releaseConnection(con);
        }
    }

    /**
     * A template method to execute a set of sqls in batch.
     */
    public int[] executeBatch(final String[] sqls) throws JdbcStoreException {
        final Connection con = DataSourceUtils.getConnection(dataSource);
        Statement statement = null;
        try {
            statement = con.createStatement();
            // statement.setQueryTimeout(settings.getQueryTimeout());
            for (final String sql : sqls) {
                statement.addBatch(sql);
            }
            return statement.executeBatch();
        } catch (final SQLException e) {
            if (log.isTraceEnabled()) {
                log.trace("Failed to execute sql [" + Arrays.toString(sqls) + "]", e);
            }
            throw new JdbcStoreException("Failed to execute [" + Arrays.toString(sqls) + "]", e);
        } finally {
            DataSourceUtils.closeStatement(statement);
            DataSourceUtils.releaseConnection(con);
        }
    }

    /**
     * A template method to execute that can execute the same sql several times
     * using different values set to it (in the
     * <code>fillPrepareStatement</code>) callback).
     */
    public int[] executeBatch(final String sql, final PrepateStatementAwareCallback callback)
            throws JdbcStoreException {
        final Connection con = DataSourceUtils.getConnection(dataSource);
        PreparedStatement ps = null;
        try {
            ps = con.prepareStatement(sql);
            // ps.setQueryTimeout(settings.getQueryTimeout());
            callback.fillPrepareStatement(ps);
            return ps.executeBatch();
        } catch (final JdbcStoreException e) {
            if (log.isTraceEnabled()) {
                log.trace("Failed to execute sql [" + sql + "]", e);
            }
            throw e;
        } catch (final Exception e) {
            if (log.isTraceEnabled()) {
                log.trace("Failed to execute sql [" + sql + "]", e);
            }
            throw new JdbcStoreException("Failed to execute sql [" + sql + "]", e);
        } finally {
            DataSourceUtils.closeStatement(ps);
            DataSourceUtils.releaseConnection(con);
        }
    }
}
