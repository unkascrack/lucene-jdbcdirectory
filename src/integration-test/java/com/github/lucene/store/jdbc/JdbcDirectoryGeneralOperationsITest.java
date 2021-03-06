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
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.github.lucene.store.jdbc.datasource.DataSourceUtils;
import com.github.lucene.store.jdbc.handler.ActualDeleteFileEntryHandler;
import com.github.lucene.store.jdbc.support.JdbcTable;
import com.github.lucene.store.jdbc.support.JdbcTemplate;

/**
 * @author kimchy
 */
public class JdbcDirectoryGeneralOperationsITest extends AbstractJdbcDirectoryITest {

    private JdbcDirectory jdbcDirectory;
    private JdbcTemplate jdbcTemplate;

    @Before
    public void setUp() throws Exception {
        final JdbcDirectorySettings settings = new JdbcDirectorySettings();
        settings.getDefaultFileEntrySettings().setClassSetting(JdbcFileEntrySettings.FILE_ENTRY_HANDLER_TYPE,
                ActualDeleteFileEntryHandler.class);
        jdbcDirectory = new JdbcDirectory(dataSource, new JdbcTable(settings, createDialect(), "TEST"));
        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @After
    public void tearDown() throws Exception {
        jdbcDirectory.close();
    }

    @Test
    public void testCreateDelteExists() throws IOException {
        jdbcDirectory.create();

        if (jdbcDirectory.getDialect().supportsTableExists()) {
            Assert.assertTrue(jdbcDirectory.tableExists());
        }

        jdbcDirectory.delete();

        if (jdbcDirectory.getDialect().supportsTableExists()) {
            Assert.assertFalse(jdbcDirectory.tableExists());
        }
    }

    @Test
    public void testCreateDelteExistsWitinTransaction() throws IOException {
        jdbcDirectory.create();

        if (jdbcDirectory.getDialect().supportsTableExists()) {
            Assert.assertTrue(jdbcDirectory.tableExists());
        }

        jdbcTemplate.executeSelect("select * from test", new JdbcTemplate.ExecuteSelectCallback() {

            @Override
            public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
            }

            @Override
            public Object execute(final ResultSet rs) throws Exception {
                Assert.assertFalse(rs.next());
                return null;
            }
        });

        jdbcDirectory.delete();

        if (jdbcDirectory.getDialect().supportsTableExists()) {
            Assert.assertFalse(jdbcDirectory.tableExists());
        }

        try {
            jdbcTemplate.executeSelect("select * from test", new JdbcTemplate.ExecuteSelectCallback() {

                @Override
                public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                }

                @Override
                public Object execute(final ResultSet rs) throws Exception {
                    Assert.assertFalse(rs.next());
                    return null;
                }
            });
            Assert.fail();
        } catch (final Exception e) {

        }
    }

    @Test
    public void testList() throws IOException {
        Connection con = DataSourceUtils.getConnection(dataSource);
        jdbcDirectory.create();
        DataSourceUtils.commitConnectionIfPossible(con);
        DataSourceUtils.releaseConnection(con);

        con = DataSourceUtils.getConnection(dataSource);
        String[] list = jdbcDirectory.listAll();
        DataSourceUtils.commitConnectionIfPossible(con);
        DataSourceUtils.releaseConnection(con);
        Assert.assertEquals(0, list.length);

        con = DataSourceUtils.getConnection(dataSource);
        final IndexOutput indexOutput = jdbcDirectory.createOutput("test1", new IOContext());
        indexOutput.writeString("TEST STRING");
        indexOutput.close();
        DataSourceUtils.commitConnectionIfPossible(con);
        DataSourceUtils.releaseConnection(con);

        jdbcTemplate.executeSelect("select * from test", new JdbcTemplate.ExecuteSelectCallback() {

            @Override
            public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
            }

            @Override
            public Object execute(final ResultSet rs) throws Exception {
                Assert.assertTrue(rs.next());
                return null;
            }
        });

        con = DataSourceUtils.getConnection(dataSource);
        list = jdbcDirectory.listAll();
        DataSourceUtils.commitConnectionIfPossible(con);
        DataSourceUtils.releaseConnection(con);
        Assert.assertEquals(1, list.length);

        con = DataSourceUtils.getConnection(dataSource);
        jdbcDirectory.deleteFile("test1");
        DataSourceUtils.commitConnectionIfPossible(con);
        DataSourceUtils.releaseConnection(con);

        con = DataSourceUtils.getConnection(dataSource);
        list = jdbcDirectory.listAll();
        DataSourceUtils.commitConnectionIfPossible(con);
        DataSourceUtils.releaseConnection(con);
        Assert.assertEquals(0, list.length);
    }

    @Test
    public void testListWithinTransaction() throws IOException {
        final Connection con = DataSourceUtils.getConnection(dataSource);

        jdbcDirectory.create();

        String[] list = jdbcDirectory.listAll();
        Assert.assertEquals(0, list.length);

        final IndexOutput indexOutput = jdbcDirectory.createOutput("test1", new IOContext());
        indexOutput.writeString("TEST STRING");
        indexOutput.close();

        jdbcTemplate.executeSelect("select * from test", new JdbcTemplate.ExecuteSelectCallback() {

            @Override
            public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
            }

            @Override
            public Object execute(final ResultSet rs) throws Exception {
                Assert.assertTrue(rs.next());
                return null;
            }
        });

        list = jdbcDirectory.listAll();
        Assert.assertEquals(1, list.length);

        jdbcDirectory.deleteFile("test1");
        list = jdbcDirectory.listAll();
        Assert.assertEquals(0, list.length);

        DataSourceUtils.rollbackConnectionIfPossible(con);
        DataSourceUtils.releaseConnection(con);
    }

    @Test
    public void testDeleteContent() throws IOException {
        Connection con = DataSourceUtils.getConnection(dataSource);
        jdbcDirectory.create();
        DataSourceUtils.commitConnectionIfPossible(con);
        DataSourceUtils.releaseConnection(con);

        con = DataSourceUtils.getConnection(dataSource);
        String[] list = jdbcDirectory.listAll();
        DataSourceUtils.commitConnectionIfPossible(con);
        DataSourceUtils.releaseConnection(con);
        Assert.assertEquals(0, list.length);

        con = DataSourceUtils.getConnection(dataSource);
        final IndexOutput indexOutput = jdbcDirectory.createOutput("test1", new IOContext());
        indexOutput.writeString("TEST STRING");
        indexOutput.close();
        DataSourceUtils.commitConnectionIfPossible(con);
        DataSourceUtils.releaseConnection(con);

        con = DataSourceUtils.getConnection(dataSource);
        list = jdbcDirectory.listAll();
        DataSourceUtils.commitConnectionIfPossible(con);
        DataSourceUtils.releaseConnection(con);
        Assert.assertEquals(1, list.length);

        con = DataSourceUtils.getConnection(dataSource);
        jdbcDirectory.deleteContent();
        DataSourceUtils.commitConnectionIfPossible(con);
        DataSourceUtils.releaseConnection(con);

        con = DataSourceUtils.getConnection(dataSource);
        list = jdbcDirectory.listAll();
        DataSourceUtils.commitConnectionIfPossible(con);
        DataSourceUtils.releaseConnection(con);
        Assert.assertEquals(0, list.length);
    }
}
