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

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.store.Lock;

import com.github.lucene.store.jdbc.handler.ActualDeleteFileEntryHandler;
import com.github.lucene.store.jdbc.handler.NoOpFileEntryHandler;
import com.github.lucene.store.jdbc.index.FetchOnOpenJdbcIndexInput;
import com.github.lucene.store.jdbc.index.RAMJdbcIndexOutput;
import com.github.lucene.store.jdbc.lock.PhantomReadLock;

/**
 * General directory level settings.
 * <p />
 * The settings also holds {@link JdbcFileEntrySettings}, that can be registered
 * with the directory settings. Note, that when registering them, they are
 * registered under both the complete name and the 3 charecters name suffix.
 * <p />
 * When creating the settings, it already holds sensible settings, they are: The
 * default {@link JdbcFileEntrySettings} uses the file entry settings defaults.
 * The "deletable", ""deleteable.new", and "deletable.new" uses the
 * {@link org.apache.lucene.store.jdbc.handler.NoOpFileEntryHandler}. The
 * "segments" and "segments.new" uses the
 * {@link org.apache.lucene.store.jdbc.handler.ActualDeleteFileEntryHandler},
 * {@link org.apache.lucene.store.jdbc.index.FetchOnOpenJdbcIndexInput}, and
 * {@link org.apache.lucene.store.jdbc.index.RAMJdbcIndexOutput}. The file
 * suffix "fnm" uses the
 * {@link org.apache.lucene.store.jdbc.index.FetchOnOpenJdbcIndexInput}, and
 * {@link org.apache.lucene.store.jdbc.index.RAMJdbcIndexOutput}. The file
 * suffix "del" and "tmp" uses the
 * {@link org.apache.lucene.store.jdbc.handler.ActualDeleteFileEntryHandler}.
 *
 * @author kimchy
 */
public class JdbcDirectorySettings {

    /**
     * The default file entry settings name that are registered under.
     */
    public static String DEFAULT_FILE_ENTRY = "__default__";

    /**
     * A simple constant having the millisecond value of an hour.
     */
    public static final long HOUR = 60 * 60 * 1000;

    private int nameColumnLength = 50;

    private int valueColumnLengthInK = 500 * 1000;

    private String nameColumnName = "name_";

    private String valueColumnName = "value_";

    private String sizeColumnName = "size_";

    private String lastModifiedColumnName = "lf_";

    private String deletedColumnName = "deleted_";

    private final HashMap<String, JdbcFileEntrySettings> fileEntrySettings = new HashMap<String, JdbcFileEntrySettings>();

    private long deleteMarkDeletedDelta = HOUR;

    private int queryTimeout = 10;

    private Class<? extends Lock> lockClass = PhantomReadLock.class;

    private String tableCatalog = null;

    private String tableSchema = null;

    private String tableType = "";

    /**
     * Creates a new instance of the Jdbc directory settings with it's default
     * values initialized.
     */
    public JdbcDirectorySettings() {
        final JdbcFileEntrySettings defaultSettings = new JdbcFileEntrySettings();
        registerFileEntrySettings(DEFAULT_FILE_ENTRY, defaultSettings);

        final JdbcFileEntrySettings deletableSettings = new JdbcFileEntrySettings();
        deletableSettings.setClassSetting(JdbcFileEntrySettings.FILE_ENTRY_HANDLER_TYPE, NoOpFileEntryHandler.class);
        registerFileEntrySettings("deletable", deletableSettings);
        registerFileEntrySettings("deleteable.new", deletableSettings);
        // in case lucene fix the spelling mistake
        registerFileEntrySettings("deletable.new", deletableSettings);

        final JdbcFileEntrySettings segmentsSettings = new JdbcFileEntrySettings();
        segmentsSettings.setClassSetting(JdbcFileEntrySettings.FILE_ENTRY_HANDLER_TYPE,
                ActualDeleteFileEntryHandler.class);
        segmentsSettings.setClassSetting(JdbcFileEntrySettings.INDEX_INPUT_TYPE_SETTING,
                FetchOnOpenJdbcIndexInput.class);
        segmentsSettings.setClassSetting(JdbcFileEntrySettings.INDEX_OUTPUT_TYPE_SETTING, RAMJdbcIndexOutput.class);
        registerFileEntrySettings("segments", segmentsSettings);
        registerFileEntrySettings("segments.new", segmentsSettings);

        final JdbcFileEntrySettings dotDelSettings = new JdbcFileEntrySettings();
        dotDelSettings.setClassSetting(JdbcFileEntrySettings.FILE_ENTRY_HANDLER_TYPE,
                ActualDeleteFileEntryHandler.class);
        registerFileEntrySettings("del", dotDelSettings);

        final JdbcFileEntrySettings tmpSettings = new JdbcFileEntrySettings();
        tmpSettings.setClassSetting(JdbcFileEntrySettings.FILE_ENTRY_HANDLER_TYPE, ActualDeleteFileEntryHandler.class);
        registerFileEntrySettings("tmp", dotDelSettings);

        final JdbcFileEntrySettings fnmSettings = new JdbcFileEntrySettings();
        fnmSettings.setClassSetting(JdbcFileEntrySettings.INDEX_INPUT_TYPE_SETTING, FetchOnOpenJdbcIndexInput.class);
        fnmSettings.setClassSetting(JdbcFileEntrySettings.INDEX_OUTPUT_TYPE_SETTING, RAMJdbcIndexOutput.class);
        registerFileEntrySettings("fnm", fnmSettings);
    }

    /**
     * Returns the name column length.
     */
    public int getNameColumnLength() {
        return nameColumnLength;
    }

    /**
     * Sets the name column length.
     */
    public void setNameColumnLength(final int nameColumnLength) {
        this.nameColumnLength = nameColumnLength;
    }

    /**
     * Returns the value column length (In K).
     */
    public int getValueColumnLengthInK() {
        return valueColumnLengthInK;
    }

    /**
     * Sets the value coumn length (In K).
     */
    public void setValueColumnLengthInK(final int valueColumnLengthInK) {
        this.valueColumnLengthInK = valueColumnLengthInK;
    }

    /**
     * Returns the name column name (defaults to name_).
     */
    public String getNameColumnName() {
        return nameColumnName;
    }

    /**
     * Sets the name column name.
     */
    public void setNameColumnName(final String nameColumnName) {
        this.nameColumnName = nameColumnName;
    }

    /**
     * Returns the value column name (defaults to value_).
     */
    public String getValueColumnName() {
        return valueColumnName;
    }

    /**
     * Sets the value column name.
     */
    public void setValueColumnName(final String valueColumnName) {
        this.valueColumnName = valueColumnName;
    }

    /**
     * Returns the size column name (default to size_).
     */
    public String getSizeColumnName() {
        return sizeColumnName;
    }

    /**
     * Sets the size column name.
     */
    public void setSizeColumnName(final String sizeColumnName) {
        this.sizeColumnName = sizeColumnName;
    }

    /**
     * Returns the last modified column name (defaults to lf_).
     */
    public String getLastModifiedColumnName() {
        return lastModifiedColumnName;
    }

    /**
     * Sets the last modified column name.
     */
    public void setLastModifiedColumnName(final String lastModifiedColumnName) {
        this.lastModifiedColumnName = lastModifiedColumnName;
    }

    /**
     * Returns the deleted column name (defaults to deleted_).
     */
    public String getDeletedColumnName() {
        return deletedColumnName;
    }

    /**
     * Sets the deleted column name.
     */
    public void setDeletedColumnName(final String deletedColumnName) {
        this.deletedColumnName = deletedColumnName;
    }

    /**
     * Registers a {@link JdbcFileEntrySettings} against the given name. The
     * name can be the full name of the file, or it's 3 charecters suffix.
     */
    public void registerFileEntrySettings(final String name, final JdbcFileEntrySettings fileEntrySettings) {
        this.fileEntrySettings.put(name, fileEntrySettings);
    }

    /**
     * Returns the file entries map. Please don't change it during runtime.
     */
    public Map<String, JdbcFileEntrySettings> getFileEntrySettings() {
        return fileEntrySettings;
    }

    /**
     * Returns the file entries according to the name. If a direct match is
     * found, it's registered {@link JdbcFileEntrySettings} is returned. If one
     * is registered against the last 3 charecters, then it is returned. If none
     * is found, the default file entry handler is returned.
     */
    public JdbcFileEntrySettings getFileEntrySettings(final String name) {
        final JdbcFileEntrySettings settings = getFileEntrySettingsWithoutDefault(name);
        if (settings != null) {
            return settings;
        }
        return getDefaultFileEntrySettings();
    }

    /**
     * Same as {@link #getFileEntrySettings(String)}, only returns
     * <code>null</code> if no match is found (instead of the default file entry
     * handler settings).
     */
    public JdbcFileEntrySettings getFileEntrySettingsWithoutDefault(final String name) {
        final JdbcFileEntrySettings settings = fileEntrySettings.get(name.substring(name.length() - 3));
        if (settings != null) {
            return settings;
        }
        return fileEntrySettings.get(name);
    }

    /**
     * Returns the default file entry handler settings.
     */
    public JdbcFileEntrySettings getDefaultFileEntrySettings() {
        return fileEntrySettings.get(DEFAULT_FILE_ENTRY);
    }

    /**
     * Returns the delta (in millis) for the delete mark deleted. File entries
     * marked as being deleted will be deleted from the system (using
     * {@link JdbcDirectory#deleteMarkDeleted()} if: current_time -
     * deletelMarkDeletedDelta &lt; Time File Entry Marked as Deleted.
     */
    public long getDeleteMarkDeletedDelta() {
        return deleteMarkDeletedDelta;
    }

    /**
     * Sets the delta (in millis) for the delete mark deleted. File entries
     * marked as being deleted will be deleted from the system (using
     * {@link JdbcDirectory#deleteMarkDeleted()} if: current_time -
     * deletelMarkDeletedDelta &lt; Time File Entry Marked as Deleted.
     */
    public void setDeleteMarkDeletedDelta(final long deleteMarkDeletedDelta) {
        this.deleteMarkDeletedDelta = deleteMarkDeletedDelta;
    }

    /**
     * Query timeout applies to Jdbc queries.
     */
    public int getQueryTimeout() {
        return queryTimeout;
    }

    /**
     * Query timeout applies to Jdbc queries.
     */
    public void setQueryTimeout(final int queryTimeout) {
        this.queryTimeout = queryTimeout;
    }

    /**
     * Returns the lock class that will be used for locking. Defaults to
     * {@link org.apache.lucene.store.jdbc.lock.PhantomReadLock}.
     */
    public Class<? extends Lock> getLockClass() {
        return lockClass;
    }

    /**
     * Sets the lock class that will be used for locking. Defaults to
     * {@link org.apache.lucene.store.jdbc.lock.PhantomReadLock}.
     */
    public void setLockClass(final Class<? extends Lock> lockClass) {
        this.lockClass = lockClass;
    }

    public String getTableCatalog() {
        return tableCatalog;
    }

    public void setTableCatalog(final String tableCatalog) {
        this.tableCatalog = tableCatalog;
    }

    public String getTableSchema() {
        return tableSchema;
    }

    public void setTableSchema(final String tableSchema) {
        this.tableSchema = tableSchema;
    }

    public String getTableType() {
        return tableType;
    }

    public void setTableType(final String tableType) {
        this.tableType = tableType;
    }
}
