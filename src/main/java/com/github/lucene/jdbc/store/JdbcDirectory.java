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

package com.github.lucene.jdbc.store;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;

/**
 * A Jdbc based implementation of a Lucene <code>Directory</code> allowing the storage of a Lucene index within a
 * database. Uses a jdbc <code>DataSource</code>, {@link org.apache.lucene.store.jdbc.dialect.Dialect} specific for the
 * database used, and an optional {@link JdbcDirectorySettings} and
 * {@link org.apache.lucene.store.jdbc.support.JdbcTable} for configuration.
 * <p/>
 * The directory works against a single table, where the binary data is stored in <code>Blob</code>. Each "file" has an
 * entry in the database, and different {@link org.apache.lucene.store.jdbc.handler.FileEntryHandler} can be defines for
 * different files (or files groups).
 * <p/>
 * Most of the files will not be deleted from the database when the directory delete method is called, but will only be
 * marked to be deleted (see {@link org.apache.lucene.store.jdbc.handler.MarkDeleteFileEntryHandler}. It is done since
 * other readers or searchers might be working with the database, and still use the files. The ability to purge mark
 * deleted files based on a "delta" is acheived using {@link #deleteMarkDeleted()} and {@link #deleteMarkDeleted(long)}.
 * Note, the purging process is not called by the directory code, so it will have to be managed by the application using
 * the jdbc directory.
 * <p/>
 * For transaction management, all the operations performed against the database do not call <code>commit</code> or
 * <code>rollback</code>. They simply open a connection (using
 * {@link org.apache.lucene.store.jdbc.datasource.DataSourceUtils#getConnection(javax.sql.DataSource)} ), and close it
 * using {@link org.apache.lucene.store.jdbc.datasource.DataSourceUtils#releaseConnection(java.sql.Connection)}). This
 * results in the fact that transcation management is simple and wraps the directory operations, allowing it to span as
 * many operations as needed.
 * <p/>
 * For none managed applications (i.e. applications that do not use JTA or Spring transaction manager), the jdbc
 * directory implementation comes with {@link org.apache.lucene.store.jdbc.datasource.TransactionAwareDataSourceProxy}
 * which wraps a <code>DataSource</code> (should be a pooled one, like Jakartat DBCP). Using it with the
 * {@link org.apache.lucene.store.jdbc.datasource.DataSourceUtils}, or the provided {@link DirectoryTemplate} should
 * make integrating or using jdbc directory simple.
 * <p/>
 * Also, for none managed applications, there is an option working with autoCommit=true mode. The system will work much
 * slower, and it is only supported on a portion of the databases, but any existing code that uses Lucene with any other
 * <code>Directory</code> implemenation should work as is.
 * <p/>
 * If working within managed environments, an external transaction management should be performed (using JTA for
 * example). Simple solutions can be using CMT or Spring Framework abstraction of transaction managers. Currently, the
 * jdbc directory implementation does not implement a transaction management abstraction, since there is a very good
 * solution out there already (Spring and JTA). Note, when using Spring and the
 * <code>DataSourceTransactionManager</code>, to provide the jdbc directory with a Spring's
 * <code>TransactionAwareDataSourceProxy</code>.
 *
 * @author kimchy
 */
public class JdbcDirectory extends Directory {

    private Dialect dialect;

    private DataSource dataSource;

    private JdbcTable table;

    private JdbcDirectorySettings settings;

    private final HashMap<String, FileEntryHandler> fileEntryHandlers = new HashMap<String, FileEntryHandler>();

    private JdbcTemplate jdbcTemplate;

    /**
     * Creates a new jdbc directory. Creates new {@link JdbcDirectorySettings} using it's default values. Uses
     * {@link org.apache.lucene.store.jdbc.dialect.DialectResolver} to try and automatically reolve the
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
     * Creates a new jdbc directory. Creates new {@link JdbcDirectorySettings} using it's default values.
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
     * Creates a new jdbc directory. Uses {@link org.apache.lucene.store.jdbc.dialect.DialectResolver} to try and
     * automatically reolve the {@link org.apache.lucene.store.jdbc.dialect.Dialect}.
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
        jdbcTemplate = new JdbcTemplate(dataSource, table.getSettings());
        dialect = table.getDialect();
        this.table = table;
        settings = table.getSettings();
        dialect.processSettings(settings);
        final Map fileEntrySettings = settings.getFileEntrySettings();
        // go over all the file entry settings and configure them
        for (final Iterator it = fileEntrySettings.keySet().iterator(); it.hasNext();) {
            final String name = (String) it.next();
            final JdbcFileEntrySettings feSettings = (JdbcFileEntrySettings) fileEntrySettings.get(name);
            try {
                final Class fileEntryHandlerClass = feSettings.getSettingAsClass(
                        JdbcFileEntrySettings.FILE_ENTRY_HANDLER_TYPE, null);
                final FileEntryHandler fileEntryHandler = (FileEntryHandler) fileEntryHandlerClass.newInstance();
                fileEntryHandler.configure(this);
                fileEntryHandlers.put(name, fileEntryHandler);
            } catch (final Exception e) {
                throw new IllegalArgumentException("Failed to create FileEntryHandler  ["
                        + feSettings.getSetting(JdbcFileEntrySettings.FILE_ENTRY_HANDLER_TYPE) + "]");
            }
        }
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public IndexOutput createOutput(final String name, final IOContext context) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void deleteFile(final String name) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public long fileLength(final String name) throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String[] listAll() throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Lock obtainLock(final String name) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IndexInput openInput(final String name, final IOContext context) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void renameFile(final String source, final String dest) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void sync(final Collection<String> names) throws IOException {
        // TODO Auto-generated method stub

    }
}
