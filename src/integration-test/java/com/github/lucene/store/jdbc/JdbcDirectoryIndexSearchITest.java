package com.github.lucene.store.jdbc;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JdbcDirectoryIndexSearchITest extends AbstractJdbcDirectoryITest {

    private JdbcDirectory jdbcDirectory;

    @Before
    public void setUp() throws Exception {
        jdbcDirectory = new JdbcDirectory(dataSource, createDialect(), "TEST");
        jdbcDirectory.create();
    }

    @After
    public void tearDown() throws Exception {
        jdbcDirectory.close();
    }

    @Test
    public void testSearch() throws IOException, ParseException {
        // To store an index on disk, use this instead:
        // Directory directory = FSDirectory.open("/tmp/testindex");
        final IndexWriterConfig config = new IndexWriterConfig(analyzer);
        final IndexWriter iwriter = new IndexWriter(jdbcDirectory, config);
        final Document doc = new Document();
        final String text = "This is the text to be indexed.";
        doc.add(new Field("fieldname", text, TextField.TYPE_STORED));
        iwriter.addDocument(doc);
        iwriter.close();

        // Now search the index:
        final DirectoryReader ireader = DirectoryReader.open(jdbcDirectory);
        final IndexSearcher isearcher = new IndexSearcher(ireader);
        // Parse a simple query that searches for "text":

        final QueryParser parser = new QueryParser("fieldname", analyzer);
        final Query query = parser.parse("text");
        final ScoreDoc[] hits = isearcher.search(query, null, 1000).scoreDocs;
        Assert.assertEquals(1, hits.length);
        // Iterate through the results:
        for (final ScoreDoc hit : hits) {
            final Document hitDoc = isearcher.doc(hit.doc);
            Assert.assertEquals("This is the text to be indexed.", hitDoc.get("fieldname"));
        }
        ireader.close();
    }
}
