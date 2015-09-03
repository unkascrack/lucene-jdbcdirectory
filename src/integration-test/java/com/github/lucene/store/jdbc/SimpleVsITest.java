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
import java.util.ArrayList;
import java.util.Collection;

import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Before;
import org.junit.Test;

import com.github.lucene.store.DirectoryTemplate;
import com.github.lucene.store.jdbc.datasource.DataSourceUtils;

/**
 * @author kimchy
 */
public class SimpleVsITest extends AbstractJdbcDirectoryITest {

    private Directory fsDir;
    private Directory ramDir;
    private Directory jdbcDir;
    private final Collection<String> docs = loadDocuments(3000, 5);
    private final boolean useCompoundFile = false;

    @Before
    public void setUp() throws Exception {
        jdbcDir = new JdbcDirectory(dataSource, createDialect(), "TEST");
        ramDir = new RAMDirectory();
        fsDir = FSDirectory.open(FileSystems.getDefault().getPath("target/index"));

        final Connection con = DataSourceUtils.getConnection(dataSource);
        ((JdbcDirectory) jdbcDir).create();
        DataSourceUtils.commitConnectionIfPossible(con);
        DataSourceUtils.releaseConnection(con);
    }

    @Test
    public void testTiming() throws IOException {
        final long ramTiming = timeIndexWriter(ramDir);
        final long fsTiming = timeIndexWriter(fsDir);
        final long jdbcTiming = timeIndexWriter(jdbcDir);

        // Assert.assertTrue(fsTiming > ramTiming);

        System.out.println("RAMDirectory Time: " + ramTiming + " ms");
        System.out.println("FSDirectory Time : " + fsTiming + " ms");
        System.out.println("JdbcDirectory Time : " + jdbcTiming + " ms");
    }

    private long timeIndexWriter(final Directory dir) throws IOException {
        final long start = System.currentTimeMillis();
        addDocuments(dir);
        final long stop = System.currentTimeMillis();
        return stop - start;
    }

    private void addDocuments(final Directory dir) throws IOException {
        final DirectoryTemplate template = new DirectoryTemplate(dir);

        final IndexWriterConfig config = new IndexWriterConfig(new SimpleAnalyzer());
        config.setOpenMode(OpenMode.CREATE);
        config.setUseCompoundFile(useCompoundFile);

        template.execute(new DirectoryTemplate.DirectoryCallbackWithoutResult() {
            @Override
            public void doInDirectoryWithoutResult(final Directory dir) throws IOException {
                final IndexWriter writer = new IndexWriter(dir, config);
                for (final Object element : docs) {
                    final Document doc = new Document();
                    final String word = (String) element;
                    // FIXME: review
                    // doc.add(new Field("keyword", word, Field.Store.YES,
                    // Field.Index.UN_TOKENIZED));
                    // doc.add(new Field("unindexed", word, Field.Store.YES,
                    // Field.Index.NO));
                    // doc.add(new Field("unstored", word, Field.Store.NO,
                    // Field.Index.TOKENIZED));
                    // doc.add(new Field("text", word, Field.Store.YES,
                    // Field.Index.TOKENIZED));
                    doc.add(new StringField("keyword", word, Field.Store.YES));
                    doc.add(new StringField("unindexed", word, Field.Store.YES));
                    doc.add(new StringField("unstored", word, Field.Store.NO));
                    doc.add(new StringField("text", word, Field.Store.YES));

                    writer.addDocument(doc);
                }

                // FIXME: review
                // writer.optimize();
                writer.close();
            }
        });
    }

    private Collection<String> loadDocuments(final int numDocs, final int wordsPerDoc) {
        final Collection<String> docs = new ArrayList<String>(numDocs);
        for (int i = 0; i < numDocs; i++) {
            final StringBuffer doc = new StringBuffer(wordsPerDoc);
            for (int j = 0; j < wordsPerDoc; j++) {
                doc.append("Bibamus ");
            }
            docs.add(doc.toString());
        }
        return docs;
    }

}
