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

package com.github.lucene.store.jdbc;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lucene.store.DirectoryTemplate;
import com.github.lucene.store.jdbc.dialect.Dialect;
import com.github.lucene.store.jdbc.dialect.DialectResolver;
import com.github.lucene.store.jdbc.handler.FileEntryHandler;
import com.github.lucene.store.jdbc.lock.JdbcLock;
import com.github.lucene.store.jdbc.support.JdbcTable;
import com.github.lucene.store.jdbc.support.JdbcTemplate;
import com.github.lucene.store.jdbc.support.LuceneFileNames;

/**
 * A Jdbc based implementation of a Lucene <code>Directory</code> allowing the
 * storage of a Lucene index within a database. Uses a jdbc
 * <code>DataSource</code>, {@link org.apache.lucene.store.jdbc.dialect.Dialect}
 * specific for the database used, and an optional {@link JdbcDirectorySettings}
 * and {@link org.apache.lucene.store.jdbc.support.JdbcTable} for configuration.
 * <p/>
 * The directory works against a single table, where the binary data is stored
 * in <code>Blob</code>. Each "file" has an entry in the database, and different
 * {@link org.apache.lucene.store.jdbc.handler.FileEntryHandler} can be defines
 * for different files (or files groups).
 * <p/>
 * Most of the files will not be deleted from the database when the directory
 * delete method is called, but will only be marked to be deleted (see
 * {@link org.apache.lucene.store.jdbc.handler.MarkDeleteFileEntryHandler}. It
 * is done since other readers or searchers might be working with the database,
 * and still use the files. The ability to purge mark deleted files based on a
 * "delta" is acheived using {@link #deleteMarkDeleted()} and
 * {@link #deleteMarkDeleted(long)}. Note, the purging process is not called by
 * the directory code, so it will have to be managed by the application using
 * the jdbc directory.
 * <p/>
 * For transaction management, all the operations performed against the database
 * do not call <code>commit</code> or <code>rollback</code>. They simply open a
 * connection (using
 * {@link org.apache.lucene.store.jdbc.datasource.DataSourceUtils#getConnection(javax.sql.DataSource)}
 * ), and close it using
 * {@link org.apache.lucene.store.jdbc.datasource.DataSourceUtils#releaseConnection(java.sql.Connection)}
 * ). This results in the fact that transcation management is simple and wraps
 * the directory operations, allowing it to span as many operations as needed.
 * <p/>
 * For none managed applications (i.e. applications that do not use JTA or
 * Spring transaction manager), the jdbc directory implementation comes with
 * {@link org.apache.lucene.store.jdbc.datasource.TransactionAwareDataSourceProxy}
 * which wraps a <code>DataSource</code> (should be a pooled one, like Jakartat
 * DBCP). Using it with the
 * {@link org.apache.lucene.store.jdbc.datasource.DataSourceUtils}, or the
 * provided {@link DirectoryTemplate} should make integrating or using jdbc
 * directory simple.
 * <p/>
 * Also, for none managed applications, there is an option working with
 * autoCommit=true mode. The system will work much slower, and it is only
 * supported on a portion of the databases, but any existing code that uses
 * Lucene with any other <code>Directory</code> implemenation should work as is.
 * <p/>
 * If working within managed environments, an external transaction management
 * should be performed (using JTA for example). Simple solutions can be using
 * CMT or Spring Framework abstraction of transaction managers. Currently, the
 * jdbc directory implementation does not implement a transaction management
 * abstraction, since there is a very good solution out there already (Spring
 * and JTA). Note, when using Spring and the
 * <code>DataSourceTransactionManager</code>, to provide the jdbc directory with
 * a Spring's <code>TransactionAwareDataSourceProxy</code>.
 *
 * @author kimchy
 */
public class JdbcDirectory extends Directory {

    private static final Logger logger = LoggerFactory.getLogger(JdbcDirectory.class);

    private Dialect dialect;

    private DataSource dataSource;

    private JdbcTable table;

    private JdbcDirectorySettings settings;

    private final HashMap<String, FileEntryHandler> fileEntryHandlers = new HashMap<String, FileEntryHandler>();

    private JdbcTemplate jdbcTemplate;

    /**
     * Creates a new jdbc directory. Creates new {@link JdbcDirectorySettings}
     * using it's default values. Uses
     * {@link org.apache.lucene.store.jdbc.dialect.DialectResolver} to try and
     * automatically reolve the
     * {@link org.apache.lucene.store.jdbc.dialect.Dialect}.
     *
     * @param dataSource
     *            The data source to use
     * @param tableName
     *            The table name
     * @throws JdbcStoreException
     */
    public JdbcDirectory(final DataSource dataSource, final String tableName) throws JdbcStoreException {
        final Dialect dialect = new DialectResolver().getDialect(dataSource);
        initialize(dataSource, new JdbcTable(new JdbcDirectorySettings(), dialect, tableName));
    }

    /**
     * Creates a new jdbc directory. Creates new {@link JdbcDirectorySettings}
     * using it's default values.
     *
     * @param dataSource
     *            The data source to use
     * @param dialect
     *            The dialect
     * @param tableName
     *            The table name
     */
    public JdbcDirectory(final DataSource dataSource, final Dialect dialect, final String tableName) {
        initialize(dataSource, new JdbcTable(new JdbcDirectorySettings(), dialect, tableName));
    }

    /**
     * Creates a new jdbc directory. Uses
     * {@link org.apache.lucene.store.jdbc.dialect.DialectResolver} to try and
     * automatically reolve the
     * {@link org.apache.lucene.store.jdbc.dialect.Dialect}.
     *
     * @param dataSource
     *            The data source to use
     * @param settings
     *            The settings to configure the directory
     * @param tableName
     *            The table name that will be used
     */
    public JdbcDirectory(final DataSource dataSource, final JdbcDirectorySettings settings, final String tableName)
            throws JdbcStoreException {
        final Dialect dialect = new DialectResolver().getDialect(dataSource);
        initialize(dataSource, new JdbcTable(settings, dialect, tableName));
    }

    /**
     * Creates a new jdbc directory.
     *
     * @param dataSource
     *            The data source to use
     * @param dialect
     *            The dialect
     * @param settings
     *            The settings to configure the directory
     * @param tableName
     *            The table name that will be used
     */
    public JdbcDirectory(final DataSource dataSource, final Dialect dialect, final JdbcDirectorySettings settings,
            final String tableName) {
        initialize(dataSource, new JdbcTable(settings, dialect, tableName));
    }

    /**
     * Creates a new jdbc directory.
     *
     * @param dataSource
     *            The data source to use
     * @param table
     *            The Jdbc table definitions
     */
    public JdbcDirectory(final DataSource dataSource, final JdbcTable table) {
        initialize(dataSource, table);
    }

    private void initialize(final DataSource dataSource, final JdbcTable table) {
        this.dataSource = dataSource;
        jdbcTemplate = new JdbcTemplate(dataSource);
        dialect = table.getDialect();
        this.table = table;
        settings = table.getSettings();
        dialect.processSettings(settings);
        final Map<String, JdbcFileEntrySettings> fileEntrySettings = settings.getFileEntrySettings();
        // go over all the file entry settings and configure them
        for (final String name : fileEntrySettings.keySet()) {
            final JdbcFileEntrySettings feSettings = fileEntrySettings.get(name);
            try {
                final Class<?> fileEntryHandlerClass = feSettings
                        .getSettingAsClass(JdbcFileEntrySettings.FILE_ENTRY_HANDLER_TYPE, null);
                final FileEntryHandler fileEntryHandler = (FileEntryHandler) fileEntryHandlerClass.newInstance();
                fileEntryHandler.configure(this);
                fileEntryHandlers.put(name, fileEntryHandler);
            } catch (final Exception e) {
                throw new IllegalArgumentException("Failed to create FileEntryHandler  ["
                        + feSettings.getSetting(JdbcFileEntrySettings.FILE_ENTRY_HANDLER_TYPE) + "]");
            }
        }
    }

    /***********************************************************************************************
     * CUSTOM METHODS
     ***********************************************************************************************/

    /**
     * Returns <code>true</code> if the database table exists.
     *
     * @return <code>true</code> if the database table exists,
     *         <code>false</code> otherwise
     * @throws java.io.IOException
     * @throws UnsupportedOperationException
     *             If the database dialect does not support it
     */
    public boolean tableExists() throws IOException, UnsupportedOperationException {
        final Boolean tableExists = (Boolean) jdbcTemplate.executeSelect(
                dialect.sqlTableExists(table.getCatalog(), table.getSchema()),
                new JdbcTemplate.ExecuteSelectCallback() {
                    @Override
                    public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                        ps.setFetchSize(1);
                        ps.setString(1, table.getName().toLowerCase());
                    }

                    @Override
                    public Object execute(final ResultSet rs) throws Exception {
                        if (rs.next()) {
                            return Boolean.TRUE;
                        }
                        return Boolean.FALSE;
                    }
                });
        return tableExists.booleanValue();
    }

    /**
     * @param name
     * @return
     * @throws java.io.IOException
     */
    public boolean fileExists(final String name) throws IOException {
        return getFileEntryHandler(name).fileExists(name);
    }

    /**
     * Deletes the database table (drops it) from the database.
     *
     * @throws java.io.IOException
     */
    public void delete() throws IOException {
        if (!dialect.supportsIfExistsAfterTableName() && !dialect.supportsIfExistsBeforeTableName()) {
            // there are databases where the fact that an exception was thrown,
            // invalidates the connection
            // so if they do not support "if exists" in the drop clause, we will
            // try to check first if the
            // table exists.
            if (dialect.supportsTableExists() && !tableExists()) {
                return;
            }
        }
        jdbcTemplate.executeUpdate(table.sqlDrop());
    }

    /**
     * Creates a new database table. Drops it before hand.
     *
     * @throws java.io.IOException
     */
    public void create() throws IOException {
        try {
            delete();
        } catch (final Exception e) {
            logger.warn("Could not delete database: " + e.getMessage());
        }
        jdbcTemplate.executeUpdate(table.sqlCreate());
        ((JdbcLock) createLock()).initializeDatabase(this);
    }

    /**
     * Deletes the contents of the database, except for the commit and write
     * lock.
     *
     * @throws java.io.IOException
     */
    public void deleteContent() throws IOException {
        jdbcTemplate.executeUpdate(table.sqlDeletaAll());
    }

    /**
     * Delets all the file entries that are marked to be deleted, and they were
     * marked "delta" time ago (base on database time, if possible by dialect).
     * The delta is taken from
     * {@link org.apache.lucene.store.jdbc.JdbcDirectorySettings#getDeleteMarkDeletedDelta()}
     *
     * @throws java.io.IOException
     */
    public void deleteMarkDeleted() throws IOException {
        deleteMarkDeleted(settings.getDeleteMarkDeletedDelta());
    }

    /**
     * Delets all the file entries that are marked to be deleted, and they were
     * marked "delta" time ago (base on database time, if possible by dialect).
     *
     * @param delta
     * @throws java.io.IOException
     */
    public void deleteMarkDeleted(final long delta) throws IOException {
        long currentTime = System.currentTimeMillis();
        if (dialect.supportsCurrentTimestampSelection()) {
            final String timestampSelectString = dialect.getCurrentTimestampSelectString();
            if (dialect.isCurrentTimestampSelectStringCallable()) {
                currentTime = ((Long) jdbcTemplate.executeCallable(timestampSelectString,
                        new JdbcTemplate.CallableStatementCallback() {
                            @Override
                            public void fillCallableStatement(final CallableStatement cs) throws Exception {
                                cs.registerOutParameter(1, java.sql.Types.TIMESTAMP);
                            }

                            @Override
                            public Object readCallableData(final CallableStatement cs) throws Exception {
                                final Timestamp timestamp = cs.getTimestamp(1);
                                return new Long(timestamp.getTime());
                            }
                        })).longValue();
            } else {
                currentTime = ((Long) jdbcTemplate.executeSelect(timestampSelectString,
                        new JdbcTemplate.ExecuteSelectCallback() {

                            @Override
                            public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                                // nothing to do here
                            }

                            @Override
                            public Object execute(final ResultSet rs) throws Exception {
                                rs.next();
                                final Timestamp timestamp = rs.getTimestamp(1);
                                return new Long(timestamp.getTime());
                            }
                        })).longValue();
            }
        }
        final long deleteBefore = currentTime - delta;
        jdbcTemplate.executeUpdate(table.sqlDeletaMarkDeleteByDelta(),
                new JdbcTemplate.PrepateStatementAwareCallback() {
                    @Override
                    public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                        ps.setBoolean(1, true);
                        ps.setTimestamp(2, new Timestamp(deleteBefore));
                    }
                });
    }

    /**
     * @param name
     * @throws java.io.IOException
     */
    public void forceDeleteFile(final String name) throws IOException {
        jdbcTemplate.executeUpdate(table.sqlDeleteByName(), new JdbcTemplate.PrepateStatementAwareCallback() {
            @Override
            public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                ps.setFetchSize(1);
                ps.setString(1, name);
            }
        });
    }

    /**
     * @return
     * @throws IOException
     */
    protected Lock createLock() throws IOException {
        try {
            return settings.getLockClass().newInstance();
        } catch (final Exception e) {
            throw new JdbcStoreException("Failed to create lock class [" + settings.getLockClass() + "]");
        }
    }

    /**
     * @param name
     * @return
     */
    protected FileEntryHandler getFileEntryHandler(final String name) {
        FileEntryHandler handler = fileEntryHandlers.get(name.substring(name.length() - 3));
        if (handler != null) {
            return handler;
        }
        handler = fileEntryHandlers.get(name);
        if (handler != null) {
            return handler;
        }
        return fileEntryHandlers.get(JdbcDirectorySettings.DEFAULT_FILE_ENTRY);
    }

    /***********************************************************************************************
     * DIRECTORY METHODS
     ***********************************************************************************************/

    @Override
    public String[] listAll() throws IOException {
        return (String[]) jdbcTemplate.executeSelect(table.sqlSelectNames(), new JdbcTemplate.ExecuteSelectCallback() {
            @Override
            public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                ps.setBoolean(1, false);
            }

            @Override
            public Object execute(final ResultSet rs) throws Exception {
                final ArrayList<String> names = new ArrayList<String>();
                while (rs.next()) {
                    names.add(rs.getString(1));
                }
                return names.toArray(new String[names.size()]);
            }
        });
    }

    @Override
    public void deleteFile(final String name) throws IOException {
        if (LuceneFileNames.isStaticFile(name)) {
            // TODO is necessary??
            logger.warn("JdbcDirectory.deleteFile({}), is static file", name);
            forceDeleteFile(name);
        } else {
            getFileEntryHandler(name).deleteFile(name);
        }
    }

    @Override
    public long fileLength(final String name) throws IOException {
        return getFileEntryHandler(name).fileLength(name);
    }

    @Override
    public IndexOutput createOutput(final String name, final IOContext context) throws IOException {
        if (LuceneFileNames.isStaticFile(name)) {
            // TODO is necessary??
            logger.warn("JdbcDirectory.createOutput({}), is static file", name);
            forceDeleteFile(name);
        }
        return getFileEntryHandler(name).createOutput(name);
    }

    @Override
    public IndexInput openInput(final String name, final IOContext context) throws IOException {
        return getFileEntryHandler(name).openInput(name);
    }

    @Override
    public void sync(final Collection<String> names) throws IOException {
        logger.warn("JdbcDirectory.sync()");
        for (final String name : names) {
            if (!fileExists(name)) {
                throw new JdbcStoreException("Failed to sync, file " + name + " not found");
            }
        }
    }

    @Override
    public void renameFile(final String from, final String to) throws IOException {
        getFileEntryHandler(from).renameFile(from, to);
    }

    @Override
    public Lock obtainLock(final String name) throws IOException {
        final Lock lock = createLock();
        ((JdbcLock) lock).configure(this, name);
        ((JdbcLock) lock).obtain();
        return lock;
    }

    @Override
    public void close() throws IOException {
        IOException last = null;
        for (final FileEntryHandler fileEntryHandler : fileEntryHandlers.values()) {
            try {
                fileEntryHandler.close();
            } catch (final IOException e) {
                last = e;
            }
        }
        if (last != null) {
            throw last;
        }
    }

    /***********************************************************************************************
     * SETTER/GETTERS METHODS
     ***********************************************************************************************/

    public Dialect getDialect() {
        return dialect;
    }

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    public JdbcTable getTable() {
        return table;
    }

    public JdbcDirectorySettings getSettings() {
        return settings;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

}
