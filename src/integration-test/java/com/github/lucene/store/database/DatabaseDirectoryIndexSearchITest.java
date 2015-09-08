package com.github.lucene.store.database;

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.github.lucene.store.AbstractContextIntegrationTests;

public class DatabaseDirectoryIndexSearchITest extends AbstractContextIntegrationTests {

    private Directory directory;

    private final Collection<String> docs = loadDocuments(3000, 5);
    private final OpenMode openMode = OpenMode.CREATE;
    private final boolean useCompoundFile = false;

    @Before
    public void initDirectory() throws DatabaseStoreException, IOException {
        directory = new DatabaseDirectory(dataSource, dialect, indexTableName);
        // directory =
        // FSDirectory.open(FileSystems.getDefault().getPath("target/index"));
        // create empty index
        final IndexWriterConfig config = getIndexWriterConfig(analyzer, openMode, useCompoundFile);
        final IndexWriter writer = new IndexWriter(directory, config);
        writer.close();
    }

    @After
    public void closeDirectory() throws IOException {
        directory.close();
    }

    @Test
    public void testSearch_whenIndexIsEmpty_shouldNoFoundResults() throws IOException, ParseException {
        final DirectoryReader reader = DirectoryReader.open(directory);
        final IndexSearcher isearcher = new IndexSearcher(reader);
        // Parse a simple query that searches for "text":

        final QueryParser parser = new QueryParser("fieldname", analyzer);
        final Query query = parser.parse("text");
        final ScoreDoc[] hits = isearcher.search(query, null, 1000).scoreDocs;
        Assert.assertEquals(0, hits.length);
        reader.close();
    }
}
