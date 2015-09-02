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
import java.nio.file.Path;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;

import com.github.lucene.store.jdbc.JdbcDirectory;
import com.github.lucene.store.jdbc.datasource.DataSourceUtils;

/**
 * @author kimchy
 */
public class SimpleVsITest extends AbstractJdbcDirectoryITest {

    private final boolean DISABLE = true;

    private Directory fsDir;
    private Directory ramDir;
    private Directory jdbcDir;
    private final Collection docs = loadDocuments(3000, 5);
    private final boolean useCompoundFile = false;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        ramDir = new RAMDirectory();

        final Path path = FileSystems.getDefault().getPath("target/index");

        fsDir = FSDirectory.open(path);
        jdbcDir = new JdbcDirectory(dataSource, createDialect(), "TEST");

        final Connection con = DataSourceUtils.getConnection(dataSource);
        ((JdbcDirectory) jdbcDir).create();
        DataSourceUtils.commitConnectionIfPossible(con);
        DataSourceUtils.releaseConnection(con);
    }

    public void testTiming() throws IOException {
        if (DISABLE) {
            return;
        }
        final long ramTiming = timeIndexWriter(ramDir);
        final long fsTiming = timeIndexWriter(fsDir);
        final long jdbcTiming = timeIndexWriter(jdbcDir);

        assertTrue(fsTiming > ramTiming);

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
        template.execute(new DirectoryTemplate.DirectoryCallbackWithoutResult() {
            protected void doInDirectoryWithoutResult(final Directory dir) throws IOException {
                final IndexWriter writer = new IndexWriter(dir, new SimpleAnalyzer(), true);
                writer.setUseCompoundFile(useCompoundFile);

                /**
                 * // change to adjust performance of indexing with FSDirectory
                 * writer.mergeFactor = writer.mergeFactor; writer.maxMergeDocs
                 * = writer.maxMergeDocs; writer.minMergeDocs =
                 * writer.minMergeDocs;
                 */

                for (final Iterator iter = docs.iterator(); iter.hasNext();) {
                    final Document doc = new Document();
                    final String word = (String) iter.next();
                    doc.add(new Field("keyword", word, Field.Store.YES, Field.Index.UN_TOKENIZED));
                    doc.add(new Field("unindexed", word, Field.Store.YES, Field.Index.NO));
                    doc.add(new Field("unstored", word, Field.Store.NO, Field.Index.TOKENIZED));
                    doc.add(new Field("text", word, Field.Store.YES, Field.Index.TOKENIZED));
                    writer.addDocument(doc);
                }
                writer.optimize();
                writer.close();
            }
        });
    }

    private Collection loadDocuments(final int numDocs, final int wordsPerDoc) {
        final Collection docs = new ArrayList(numDocs);
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
