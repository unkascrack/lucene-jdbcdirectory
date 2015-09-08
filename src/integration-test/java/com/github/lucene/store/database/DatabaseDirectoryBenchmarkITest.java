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

package com.github.lucene.store.database;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Before;
import org.junit.Test;

import com.github.lucene.store.AbstractContextIntegrationTests;

/**
 * @author kimchy
 */
public class DatabaseDirectoryBenchmarkITest extends AbstractContextIntegrationTests {

    private Directory fsDirectory;
    private Directory ramDirectory;
    private Directory databaseDirectory;

    private final Collection<String> docs = loadDocuments(3000, 5);
    private final OpenMode openMode = OpenMode.CREATE;
    private final boolean useCompoundFile = false;

    @Before
    public void setUp() throws Exception {
        ramDirectory = new RAMDirectory();
        fsDirectory = FSDirectory.open(FileSystems.getDefault().getPath("target/index"));
        databaseDirectory = new DatabaseDirectory(dataSource, dialect, "TEST");
    }

    @Test
    public void testTiming() throws IOException {
        final long ramTiming = timeIndexWriter(ramDirectory);
        final long fsTiming = timeIndexWriter(fsDirectory);
        final long databaseTiming = timeIndexWriter(databaseDirectory);

        System.out.println("RAMDirectory Time: " + ramTiming + " ms");
        System.out.println("FSDirectory Time : " + fsTiming + " ms");
        System.out.println("DatabaseDirectory Time : " + databaseTiming + " ms");
    }

    private long timeIndexWriter(final Directory dir) throws IOException {
        final long start = System.currentTimeMillis();
        addDocuments(dir, openMode, useCompoundFile, docs);
        final long stop = System.currentTimeMillis();
        return stop - start;
    }

    protected Collection<String> loadDocuments(final int numDocs, final int wordsPerDoc) {
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

    protected void addDocuments(final Directory directory, final OpenMode openMode, final boolean useCompoundFile,
            final Collection<String> docs) throws IOException {
        final IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setOpenMode(OpenMode.CREATE);
        config.setUseCompoundFile(useCompoundFile);

        final IndexWriter writer = new IndexWriter(directory, config);
        for (final Object element : docs) {
            final Document doc = new Document();
            final String word = (String) element;
            doc.add(new StringField("index_store_unanalyzed", word, Field.Store.YES));
            doc.add(new StoredField("unindexed_store_unanalyzed", word));
            doc.add(new StringField("index_unstore_unanalyzed", word, Field.Store.NO));
            doc.add(new TextField("index_store_analyzed", word, Field.Store.YES));
            doc.add(new TextField("index_unstore_analyzed", word, Field.Store.NO));
            writer.addDocument(doc);
        }
        writer.forceMerge(1);
        writer.close();

        // final DirectoryTemplate template = new DirectoryTemplate(directory);
        // template.execute(new
        // DirectoryTemplate.DirectoryCallbackWithoutResult() {
        // @Override
        // public void doInDirectoryWithoutResult(final Directory dir) throws
        // IOException {
        // final IndexWriter writer = new IndexWriter(dir, config);
        // for (final Object element : docs) {
        // final Document doc = new Document();
        // final String word = (String) element;
        // doc.add(new StringField("index_store_unanalyzed", word,
        // Field.Store.YES));
        // doc.add(new StoredField("unindexed_store_unanalyzed", word));
        // doc.add(new StringField("index_unstore_unanalyzed", word,
        // Field.Store.NO));
        // doc.add(new TextField("index_store_analyzed", word,
        // Field.Store.YES));
        // doc.add(new TextField("index_unstore_analyzed", word,
        // Field.Store.NO));
        // writer.addDocument(doc);
        // }
        // writer.forceMerge(1);
        // writer.close();
        // }
        // });
    }
}
