package com.github.lucene.store;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;

import javax.sql.DataSource;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.hsqldb.Server;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.server.ServerAcl.AclFormatException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.github.lucene.store.database.dialect.Dialect;
import com.github.lucene.store.database.dialect.HSQLDialect;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import net.sf.log4jdbc.sql.jdbcapi.DataSourceSpy;

public class AbstractContextIntegrationTests {

    private static Server server;

    protected final String indexTableName = "INDEX_TABLE";
    protected DataSource dataSource;
    protected Dialect dialect;
    protected Analyzer analyzer = new SimpleAnalyzer();

    @BeforeClass
    public static void initDatabase() throws IOException, AclFormatException {
        final HsqlProperties properties = new HsqlProperties();
        properties.setProperty("server.database.0", "mem:test");
        // properties.setProperty("server.database.0","file:./target/testdb");
        // properties.setProperty("server.dbname.0", "test");
        // properties.setProperty("server.port", "9001");

        server = new Server();
        server.setProperties(properties);
        server.setLogWriter(new PrintWriter(System.out));
        server.setErrWriter(new PrintWriter(System.out));
        server.start();
    }

    @AfterClass
    public static void closeDatabase() {
        server.shutdown();
    }

    @Before
    public void initDataSource() throws Exception {
        final String url = "jdbc:hsqldb:mem:test";
        final String username = "sa";
        final String password = "";

        final HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);
        config.setAutoCommit(false);
        final HikariDataSource ds = new HikariDataSource(config);
        dataSource = new DataSourceSpy(ds);
    }

    @Before
    public void initDialect() throws IOException {
        dialect = new HSQLDialect();
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
        final IndexWriterConfig config = getIndexWriterConfig(analyzer, openMode, useCompoundFile);
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
        writer.close();
    }

    protected void optimize(final Directory directory, final OpenMode openMode, final boolean useCompoundFile)
            throws IOException {
        final IndexWriterConfig config = getIndexWriterConfig(analyzer, openMode, useCompoundFile);
        final IndexWriter writer = new IndexWriter(directory, config);
        writer.forceMerge(1);
        writer.close();
    }

    protected IndexWriterConfig getIndexWriterConfig(final Analyzer analyzer, final OpenMode openMode,
            final boolean useCompoundFile) {
        final IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setOpenMode(openMode);
        config.setUseCompoundFile(useCompoundFile);
        config.setInfoStream(System.err);
        return config;
    }
}
