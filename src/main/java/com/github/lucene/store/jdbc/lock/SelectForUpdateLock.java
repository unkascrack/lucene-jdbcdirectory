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

package com.github.lucene.store.jdbc.lock;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lucene.store.jdbc.JdbcDirectory;
import com.github.lucene.store.jdbc.JdbcStoreException;
import com.github.lucene.store.jdbc.support.JdbcTemplate;

/**
 * A lock based on select...for update.
 * <p/>
 * Note, that not all databases support select ... for update, if the database
 * (dialect) does not support it, a exception will be thrown (see
 * {@link org.apache.lucene.store.jdbc.dialect.Dialect#supportsForUpdate()} .
 * <p/>
 * Also note, that when using select for update locking, when the database is
 * created, the commit and write locks will be created and the select for update
 * will be performed on them. If one wishes to switch to
 * {@link org.apache.lucene.store.jdbc.lock.PhantomReadLock}, they must be
 * manually deleted.
 * <p/>
 * The lock is released when the transaction is committed, and not when the
 * release method is called.
 *
 * @author kimchy
 */
public class SelectForUpdateLock extends Lock implements JdbcLock {

    private static final Logger logger = LoggerFactory.getLogger(SelectForUpdateLock.class);

    private JdbcDirectory jdbcDirectory;
    private String name;

    @Override
    public void configure(final JdbcDirectory jdbcDirectory, final String name) throws IOException {
        if (!jdbcDirectory.getDialect().supportsForUpdate()) {
            throw new JdbcStoreException(
                    "Database dialect [" + jdbcDirectory.getDialect() + "] does not support select for update");
        }
        this.jdbcDirectory = jdbcDirectory;
        this.name = name;
    }

    @Override
    public void initializeDatabase(final JdbcDirectory jdbcDirectory) throws IOException {
        jdbcDirectory.getJdbcTemplate().executeUpdate(jdbcDirectory.getTable().sqlInsert(),
                new JdbcTemplate.PrepateStatementAwareCallback() {
                    @Override
                    public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                        ps.setFetchSize(1);
                        ps.setString(1, IndexWriter.WRITE_LOCK_NAME);
                        ps.setNull(2, Types.BLOB);
                        ps.setLong(3, 0);
                        ps.setBoolean(4, false);
                    }
                });
    }

    @Override
    public void obtain() throws IOException {
        jdbcDirectory.getJdbcTemplate().executeSelect(jdbcDirectory.getTable().sqlSelectNameForUpdateNoWait(),
                new JdbcTemplate.ExecuteSelectCallback() {

                    @Override
                    public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                        ps.setFetchSize(1);
                        ps.setString(1, name);
                    }

                    @Override
                    public Object execute(final ResultSet rs) throws Exception {
                        if (!rs.next()) {
                            throw new LockObtainFailedException("Lock instance already obtained: " + this);
                        }
                        return null;
                    }
                });
    }

    @Override
    public void close() throws IOException {
        // no way to activly release a select for update lock
        // when the transaction commits or rolls back, it will be released
    }

    @Override
    public void ensureValid() throws IOException {
        // TODO Auto-generated method stub
        logger.debug("SelectForUpdateLock.ensureValid()");
        // TODO no way to know if it is locked or not?
        throw new IllegalStateException("SelectForUpdate lock does not support is locked");
    }

    @Override
    public String toString() {
        return "SelectForUpdateLock[" + name + "/" + jdbcDirectory.getTable() + "]";
    }
}
