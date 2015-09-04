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
import java.nio.file.FileSystems;
import java.sql.Connection;
import java.util.Collection;

import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Before;
import org.junit.Test;

import com.github.lucene.store.jdbc.datasource.DataSourceUtils;

/**
 * @author kimchy
 */
public class JdbcDirectoryBenchmarkITest extends AbstractJdbcDirectoryITest {

    private Directory fsDirectory;
    private Directory ramDirectory;
    private Directory jdbcDirectory;

    private final Collection<String> docs = loadDocuments(3000, 5);
    private final OpenMode openMode = OpenMode.CREATE;
    private final boolean useCompoundFile = false;

    @Before
    public void setUp() throws Exception {
        jdbcDirectory = new JdbcDirectory(dataSource, createDialect(), "TEST");
        ramDirectory = new RAMDirectory();
        fsDirectory = FSDirectory.open(FileSystems.getDefault().getPath("target/index"));

        final Connection con = DataSourceUtils.getConnection(dataSource);
        ((JdbcDirectory) jdbcDirectory).create();
        DataSourceUtils.commitConnectionIfPossible(con);
        DataSourceUtils.releaseConnection(con);
    }

    @Test
    public void testTiming() throws IOException {
        final long ramTiming = timeIndexWriter(ramDirectory);
        final long fsTiming = timeIndexWriter(fsDirectory);
        final long jdbcTiming = timeIndexWriter(jdbcDirectory);

        System.out.println("RAMDirectory Time: " + ramTiming + " ms");
        System.out.println("FSDirectory Time : " + fsTiming + " ms");
        System.out.println("JdbcDirectory Time : " + jdbcTiming + " ms");
    }

    private long timeIndexWriter(final Directory dir) throws IOException {
        final long start = System.currentTimeMillis();
        addDocuments(dir, openMode, useCompoundFile, docs);
        final long stop = System.currentTimeMillis();
        return stop - start;
    }

}
