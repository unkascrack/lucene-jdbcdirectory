package com.github.lucene.store;

import java.io.IOException;
import java.io.PrintWriter;

import javax.sql.DataSource;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
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
}
