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
import java.sql.Connection;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Lock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.github.lucene.store.jdbc.datasource.DataSourceUtils;
import com.github.lucene.store.jdbc.support.JdbcTable;

/**
 * @author kimchy
 */
public class JdbcDirectoryLockITest extends AbstractJdbcDirectoryITest {

    private JdbcDirectory dir1;
    private JdbcDirectory dir2;

    @Before
    public void setUp() throws Exception {
        final JdbcDirectorySettings settings = new JdbcDirectorySettings();
        settings.setQueryTimeout(1);

        dir1 = new JdbcDirectory(dataSource, new JdbcTable(settings, createDialect(), "TEST"));
        dir1.create();

        dir2 = new JdbcDirectory(dataSource, new JdbcTable(settings, createDialect(), "TEST"));
    }

    @After
    public void tearDown() throws Exception {
        dir1.close();
        dir2.close();
    }

    @Test
    public void testLocks() throws Exception {
        try {
            final Connection con1 = DataSourceUtils.getConnection(dataSource);
            final Lock lock1 = dir1.obtainLock(IndexWriter.WRITE_LOCK_NAME);

            lock1.ensureValid();

            final Connection con2 = DataSourceUtils.getConnection(dataSource);
            final Lock lock2 = dir2.obtainLock(IndexWriter.WRITE_LOCK_NAME);

            try {
                lock2.ensureValid();
                Assert.fail("lock2 should not have valid lock");
            } catch (final IOException e) {
            }

            lock1.close();

            DataSourceUtils.commitConnectionIfPossible(con1);
            DataSourceUtils.releaseConnection(con1);

            lock2.ensureValid();

            DataSourceUtils.commitConnectionIfPossible(con2);
            DataSourceUtils.releaseConnection(con2);

            // final Lock lock1 = dir1.obtainLock(IndexWriter.WRITE_LOCK_NAME);
            // lock1.ensureValid();
            //
            // final Lock lock2 = dir2.obtainLock(IndexWriter.WRITE_LOCK_NAME);
            // try {
            // lock2.ensureValid();
            // Assert.fail("lock2 should not have valid lock");
            // } catch (final IOException e) {
            // }
            //
            // lock1.close();
            //
            // try {
            // lock2.ensureValid();
            // } catch (final IOException e) {
            // Assert.fail("lock2 should have valid lock");
            // }

        } finally {
            dir1.delete();
        }
    }
}
