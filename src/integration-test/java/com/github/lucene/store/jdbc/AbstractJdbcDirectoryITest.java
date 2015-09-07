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
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;

import javax.sql.DataSource;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
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

import com.github.lucene.store.DirectoryTemplate;
import com.github.lucene.store.jdbc.dialect.Dialect;
import com.github.lucene.store.jdbc.dialect.DialectResolver;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import net.sf.log4jdbc.sql.jdbcapi.DataSourceSpy;

/**
 * @author kimchy
 */
public abstract class AbstractJdbcDirectoryITest {

    private static Server server;

    private String dialect;
    protected DataSource dataSource;

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
    public void initAttributes() throws Exception {
        final String url = "jdbc:hsqldb:mem:test";
        final String driver = "org.hsqldb.jdbcDriver";
        final String username = "sa";
        final String password = "";

        final HikariConfig config = new HikariConfig();
        config.setDriverClassName(driver);
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);
        config.setAutoCommit(true);
        final HikariDataSource ds = new HikariDataSource(config);
        dataSource = new DataSourceSpy(ds);

        // final DriverManagerDataSource ds = new
        // DriverManagerDataSource(driver, url, username,
        // password, false);
        // dataSource = new TransactionAwareDataSourceProxy(new
        // DataSourceSpy(ds));
        dialect = "com.github.lucene.store.jdbc.dialect.HSQLDialect";
    }

    protected Dialect createDialect() throws Exception {
        if (dialect == null) {
            return new DialectResolver().getDialect(dataSource);
        }
        return (Dialect) Class.forName(dialect).newInstance();
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

        final DirectoryTemplate template = new DirectoryTemplate(directory);
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
}
