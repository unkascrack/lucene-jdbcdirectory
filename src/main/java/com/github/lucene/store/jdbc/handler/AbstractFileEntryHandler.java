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

package com.github.lucene.store.jdbc.handler;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import com.github.lucene.store.jdbc.JdbcDirectory;
import com.github.lucene.store.jdbc.JdbcFileEntrySettings;
import com.github.lucene.store.jdbc.JdbcStoreException;
import com.github.lucene.store.jdbc.index.JdbcIndexConfigurable;
import com.github.lucene.store.jdbc.support.JdbcTable;
import com.github.lucene.store.jdbc.support.JdbcTemplate;

/**
 * A base file entry handler that supports most of the file entry base
 * operations.
 * <p/>
 * Supports the creation of configurable <code>IndexInput</code> and
 * <code>IndexOutput</code>, base on the
 * {@link JdbcFileEntrySettings#INDEX_INPUT_TYPE_SETTING} and
 * {@link JdbcFileEntrySettings#INDEX_OUTPUT_TYPE_SETTING}.
 * <p/>
 * Does not implement the deletion of files.
 *
 * @author kimchy
 */
public abstract class AbstractFileEntryHandler implements FileEntryHandler {

    protected JdbcDirectory jdbcDirectory;

    protected JdbcTable table;

    protected JdbcTemplate jdbcTemplate;

    @Override
    public void configure(final JdbcDirectory jdbcDirectory) {
        this.jdbcDirectory = jdbcDirectory;
        jdbcTemplate = jdbcDirectory.getJdbcTemplate();
        table = jdbcDirectory.getTable();
    }

    @Override
    public boolean fileExists(final String name) throws IOException {
        return ((Boolean) jdbcTemplate.executeSelect(table.sqlSelectNameExists(),
                new JdbcTemplate.ExecuteSelectCallback() {
                    @Override
                    public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                        ps.setFetchSize(1);
                        ps.setString(1, name);
                    }

                    @Override
                    public Object execute(final ResultSet rs) throws Exception {
                        if (!rs.next()) {
                            return Boolean.FALSE;
                        }
                        return rs.getBoolean(1) ? Boolean.FALSE : Boolean.TRUE;
                    }
                })).booleanValue();
    }

    @Override
    public long fileModified(final String name) throws IOException {
        return ((Long) jdbcTemplate.executeSelect(table.sqlSelecltLastModifiedByName(),
                new JdbcTemplate.ExecuteSelectCallback() {
                    @Override
                    public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                        ps.setFetchSize(1);
                        ps.setString(1, name);
                    }

                    @Override
                    public Object execute(final ResultSet rs) throws Exception {
                        if (rs.next()) {
                            final Timestamp ts = rs.getTimestamp(1);
                            return new Long(ts.getTime());
                        }
                        return new Long(0L);
                    }
                })).longValue();
    }

    @Override
    public void touchFile(final String name) throws IOException {
        jdbcTemplate.executeUpdate(table.sqlUpdateLastModifiedByName(),
                new JdbcTemplate.PrepateStatementAwareCallback() {
                    @Override
                    public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                        ps.setFetchSize(1);
                        ps.setString(1, name);
                    }
                });
    }

    @Override
    public void renameFile(final String from, final String to) throws IOException {
        // TODO find a way if it can be done in the same sql query
        deleteFile(to);
        jdbcTemplate.executeUpdate(table.sqlUpdateNameByName(), new JdbcTemplate.PrepateStatementAwareCallback() {
            @Override
            public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                ps.setFetchSize(1);
                ps.setString(1, to);
                ps.setString(2, from);
            }
        });
    }

    @Override
    public long fileLength(final String name) throws IOException {
        return ((Long) jdbcTemplate.executeSelect(table.sqlSelectSizeByName(),
                new JdbcTemplate.ExecuteSelectCallback() {
                    @Override
                    public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                        ps.setFetchSize(1);
                        ps.setString(1, name);
                    }

                    @Override
                    public Object execute(final ResultSet rs) throws Exception {
                        if (rs.next()) {
                            return new Long(rs.getLong(1));
                        }
                        return new Long(0L);
                    }
                })).longValue();
    }

    @Override
    public IndexInput openInput(final String name) throws IOException {
        IndexInput indexInput;
        final JdbcFileEntrySettings settings = jdbcDirectory.getSettings().getFileEntrySettings(name);
        try {
            final Class<?> inputClass = settings.getSettingAsClass(JdbcFileEntrySettings.INDEX_INPUT_TYPE_SETTING,
                    null);
            indexInput = (IndexInput) inputClass.newInstance();
        } catch (final Exception e) {
            throw new JdbcStoreException("Failed to create indexInput instance ["
                    + settings.getSetting(JdbcFileEntrySettings.INDEX_INPUT_TYPE_SETTING) + "]", e);
        }
        ((JdbcIndexConfigurable) indexInput).configure(name, jdbcDirectory, settings);
        return indexInput;
    }

    @Override
    public IndexOutput createOutput(final String name) throws IOException {
        IndexOutput indexOutput;
        final JdbcFileEntrySettings settings = jdbcDirectory.getSettings().getFileEntrySettings(name);
        try {
            final Class<?> inputClass = settings.getSettingAsClass(JdbcFileEntrySettings.INDEX_OUTPUT_TYPE_SETTING,
                    null);
            indexOutput = (IndexOutput) inputClass.newInstance();
        } catch (final Exception e) {
            throw new JdbcStoreException("Failed to create indexOutput instance ["
                    + settings.getSetting(JdbcFileEntrySettings.INDEX_OUTPUT_TYPE_SETTING) + "]", e);
        }
        ((JdbcIndexConfigurable) indexOutput).configure(name, jdbcDirectory, settings);
        return indexOutput;
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}
