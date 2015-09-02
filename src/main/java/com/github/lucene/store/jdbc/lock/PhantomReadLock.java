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
import java.sql.Types;

import org.apache.lucene.store.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lucene.store.jdbc.JdbcDirectory;
import com.github.lucene.store.jdbc.support.JdbcTemplate;

/**
 * <p>
 * A lock based on phantom reads and table level locking. For most database and
 * most transaction isolation levels this lock is suffecient.
 *
 * <p>
 * The existance of the lock in the database, marks it as being locked.
 *
 * <p>
 * The benefits of using this lock is the ability to release it.
 *
 * @author kimchy
 */
public class PhantomReadLock extends Lock implements JdbcLock {

    private static final Logger log = LoggerFactory.getLogger(PhantomReadLock.class);

    private JdbcDirectory jdbcDirectory;

    private String name;

    @Override
    public void configure(final JdbcDirectory jdbcDirectory, final String name) throws IOException {
        this.jdbcDirectory = jdbcDirectory;
        this.name = name;
    }

    @Override
    public void initializeDatabase(final JdbcDirectory jdbcDirectory) {
        // do nothing
    }

    public boolean obtain() {
        try {
            if (jdbcDirectory.getDialect().useExistsBeforeInsertLock()) {
                // there are databases where the fact that an exception was
                // thrown
                // invalidates the connection. So first we check if it exists,
                // and
                // then insert it.
                if (jdbcDirectory.fileExists(name)) {
                    return false;
                }
            }
            jdbcDirectory.getJdbcTemplate().executeUpdate(jdbcDirectory.getTable().sqlInsert(),
                    new JdbcTemplate.PrepateStatementAwareCallback() {
                        @Override
                        public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                            ps.setFetchSize(1);
                            ps.setString(1, name);
                            ps.setNull(2, Types.BLOB);
                            ps.setLong(3, 0);
                            ps.setBoolean(4, false);
                        }
                    });
        } catch (final Exception e) {
            if (log.isTraceEnabled()) {
                log.trace("Obtain Lock exception (might be valid) [" + e.getMessage() + "]");
            }
            return false;
        }
        return true;
    }

    public void release() {
        try {
            jdbcDirectory.getJdbcTemplate().executeUpdate(jdbcDirectory.getTable().sqlDeleteByName(),
                    new JdbcTemplate.PrepateStatementAwareCallback() {
                        @Override
                        public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                            ps.setFetchSize(1);
                            ps.setString(1, name);
                        }
                    });
        } catch (final Exception e) {
            if (log.isTraceEnabled()) {
                log.trace("Release Lock exception (might be valid) [" + e.getMessage() + "]");
            }
        }
    }

    public boolean isLocked() {
        try {
            return jdbcDirectory.fileExists(name);
        } catch (final Exception e) {
            return false;
        }
    }

    @Override
    public String toString() {
        return "PhantomReadLock[" + name + "/" + jdbcDirectory.getTable() + "]";
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void ensureValid() throws IOException {
        // TODO Auto-generated method stub

    }
}
