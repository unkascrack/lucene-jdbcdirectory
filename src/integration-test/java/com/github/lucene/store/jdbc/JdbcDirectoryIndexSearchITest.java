package com.github.lucene.store.jdbc;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
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

import com.github.lucene.store.DirectoryTemplate;

public class JdbcDirectoryIndexSearchITest extends AbstractJdbcDirectoryITest {

    private Directory directory;

    @Before
    public void setUp() throws Exception {
        directory = new JdbcDirectory(dataSource, createDialect(), "TEST");
        ((JdbcDirectory) directory).create();
        // directory =
        // FSDirectory.open(FileSystems.getDefault().getPath("target/index"));
    }

    @After
    public void tearDown() throws Exception {
        directory.close();
    }

    @Test
    public void testSearch() throws IOException, ParseException {
        // To store an index on disk, use this instead:
        // Directory directory = FSDirectory.open("/tmp/testindex");
        final IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setOpenMode(OpenMode.CREATE);

        // create empty index
        final IndexWriter iwriter = new IndexWriter(directory, config);
        iwriter.close();

        final DirectoryTemplate template = new DirectoryTemplate(directory);
        template.execute(new DirectoryTemplate.DirectoryCallbackWithoutResult() {
            @Override
            protected void doInDirectoryWithoutResult(final Directory dir) throws IOException {
                try {
                    final DirectoryReader ireader = DirectoryReader.open(directory);
                    final IndexSearcher isearcher = new IndexSearcher(ireader);
                    // Parse a simple query that searches for "text":

                    final QueryParser parser = new QueryParser("fieldname", analyzer);
                    final Query query = parser.parse("text");
                    final ScoreDoc[] hits = isearcher.search(query, null, 1000).scoreDocs;
                    Assert.assertEquals(0, hits.length);
                    ireader.close();
                } catch (final ParseException e) {
                    throw new IOException(e);
                }
            }

        });

        template.execute(new DirectoryTemplate.DirectoryCallbackWithoutResult() {
            @Override
            public void doInDirectoryWithoutResult(final Directory dir) throws IOException {
                final IndexWriter iwriter = new IndexWriter(directory, config);
                final Document doc = new Document();
                final String text = "This is the text to be indexed.";
                doc.add(new Field("fieldname", text, TextField.TYPE_STORED));
                iwriter.addDocument(doc);
                iwriter.close();
            }
        });

        // final IndexWriter iwriter = new IndexWriter(jdbcDirectory, config);
        // final Document doc = new Document();
        // final String text = "This is the text to be indexed.";
        // doc.add(new Field("fieldname", text, TextField.TYPE_STORED));
        // iwriter.addDocument(doc);
        // iwriter.close();

        // Now search the index:
        template.execute(new DirectoryTemplate.DirectoryCallbackWithoutResult() {
            @Override
            protected void doInDirectoryWithoutResult(final Directory dir) throws IOException {
                try {
                    final DirectoryReader ireader = DirectoryReader.open(directory);
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
                } catch (final ParseException e) {
                    throw new IOException(e);
                }
            }

        });

        // final DirectoryReader ireader = DirectoryReader.open(jdbcDirectory);
        // final IndexSearcher isearcher = new IndexSearcher(ireader);
        // // Parse a simple query that searches for "text":
        //
        // final QueryParser parser = new QueryParser("fieldname", analyzer);
        // final Query query = parser.parse("text");
        // final ScoreDoc[] hits = isearcher.search(query, null,
        // 1000).scoreDocs;
        // Assert.assertEquals(1, hits.length);
        // // Iterate through the results:
        // for (final ScoreDoc hit : hits) {
        // final Document hitDoc = isearcher.doc(hit.doc);
        // Assert.assertEquals("This is the text to be indexed.",
        // hitDoc.get("fieldname"));
        // }
        // ireader.close();
    }
}
