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

package com.github.lucene.store.jdbc.index;

import java.io.IOException;
import java.sql.Connection;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.github.lucene.store.jdbc.AbstractJdbcDirectoryITest;
import com.github.lucene.store.jdbc.JdbcDirectory;
import com.github.lucene.store.jdbc.JdbcDirectorySettings;
import com.github.lucene.store.jdbc.JdbcFileEntrySettings;
import com.github.lucene.store.jdbc.datasource.DataSourceUtils;
import com.github.lucene.store.jdbc.support.JdbcTable;

/**
 * @author kimchy
 */
public abstract class AbstractIndexInputOutputITest extends AbstractJdbcDirectoryITest {

    protected JdbcDirectory jdbcDirectory;

    @Before
    public void setUp() throws Exception {
        final JdbcDirectorySettings settings = new JdbcDirectorySettings();
        settings.getDefaultFileEntrySettings().setClassSetting(JdbcFileEntrySettings.INDEX_INPUT_TYPE_SETTING,
                indexInputClass());
        settings.getDefaultFileEntrySettings().setClassSetting(JdbcFileEntrySettings.INDEX_OUTPUT_TYPE_SETTING,
                indexOutputClass());

        jdbcDirectory = new JdbcDirectory(dataSource, new JdbcTable(settings, createDialect(), "TEST"));
        jdbcDirectory.create();
    }

    @After
    public void tearDown() throws Exception {
        jdbcDirectory.close();
    }

    protected abstract Class<? extends IndexInput> indexInputClass();

    protected abstract Class<? extends IndexOutput> indexOutputClass();

    @Test
    public void testSize5() throws IOException {
        innerTestSize(5);
    }

    @Test
    public void testSize5WithinTransaction() throws IOException {
        innertTestSizeWithinTransaction(5);
    }

    @Test
    public void testSize15() throws IOException {
        innerTestSize(15);
    }

    @Test
    public void testSize15WithinTransaction() throws IOException {
        innertTestSizeWithinTransaction(15);
    }

    @Test
    public void testSize2() throws IOException {
        innerTestSize(2);
    }

    @Test
    public void testSize2WithinTransaction() throws IOException {
        innertTestSizeWithinTransaction(2);
    }

    @Test
    public void testSize1() throws IOException {
        innerTestSize(1);
    }

    @Test
    public void testSize1WithinTransaction() throws IOException {
        innertTestSizeWithinTransaction(1);
    }

    @Test
    public void testSize50() throws IOException {
        innerTestSize(50);
    }

    @Test
    public void testSize50WithinTransaction() throws IOException {
        innertTestSizeWithinTransaction(50);
    }

    private void innerTestSize(final int bufferSize) throws IOException {
        jdbcDirectory.getSettings().getDefaultFileEntrySettings()
                .setIntSetting(JdbcBufferedIndexInput.BUFFER_SIZE_SETTING, bufferSize);
        jdbcDirectory.getSettings().getDefaultFileEntrySettings()
                .setIntSetting(JdbcBufferedIndexOutput.BUFFER_SIZE_SETTING, bufferSize);

        Connection con = DataSourceUtils.getConnection(dataSource);
        insertData();
        DataSourceUtils.commitConnectionIfPossible(con);
        DataSourceUtils.releaseConnection(con);

        con = DataSourceUtils.getConnection(dataSource);
        verifyData();
        DataSourceUtils.commitConnectionIfPossible(con);
        DataSourceUtils.releaseConnection(con);
    }

    private void innertTestSizeWithinTransaction(final int bufferSize) throws IOException {
        jdbcDirectory.getSettings().getDefaultFileEntrySettings()
                .setIntSetting(JdbcBufferedIndexInput.BUFFER_SIZE_SETTING, bufferSize);
        jdbcDirectory.getSettings().getDefaultFileEntrySettings()
                .setIntSetting(JdbcBufferedIndexOutput.BUFFER_SIZE_SETTING, bufferSize);

        final Connection con = DataSourceUtils.getConnection(dataSource);

        insertData();
        verifyData();

        DataSourceUtils.rollbackConnectionIfPossible(con);
        DataSourceUtils.releaseConnection(con);
    }

    private void insertData() throws IOException {
        final byte[] test = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        final IndexOutput indexOutput = jdbcDirectory.createOutput("value1", new IOContext());
        indexOutput.writeInt(-1);
        indexOutput.writeLong(10);
        indexOutput.writeInt(0);
        indexOutput.writeInt(0);
        indexOutput.writeBytes(test, 8);
        indexOutput.writeBytes(test, 5);
        indexOutput.writeByte((byte) 8);
        indexOutput.writeBytes(new byte[] { 1, 2 }, 2);
        indexOutput.close();
    }

    private void verifyData() throws IOException {
        final byte[] test = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        Assert.assertTrue(jdbcDirectory.fileExists("value1"));
        Assert.assertEquals(36, jdbcDirectory.fileLength("value1"));

        final IndexInput indexInput = jdbcDirectory.openInput("value1", new IOContext());
        Assert.assertEquals(-1, indexInput.readInt());
        Assert.assertEquals(10, indexInput.readLong());
        Assert.assertEquals(0, indexInput.readInt());
        Assert.assertEquals(0, indexInput.readInt());
        indexInput.readBytes(test, 0, 8);
        Assert.assertEquals((byte) 1, test[0]);
        Assert.assertEquals((byte) 8, test[7]);
        indexInput.readBytes(test, 0, 5);
        Assert.assertEquals((byte) 1, test[0]);
        Assert.assertEquals((byte) 5, test[4]);

        indexInput.seek(28);
        Assert.assertEquals((byte) 1, indexInput.readByte());
        indexInput.seek(30);
        Assert.assertEquals((byte) 3, indexInput.readByte());

        indexInput.close();
    }
}
