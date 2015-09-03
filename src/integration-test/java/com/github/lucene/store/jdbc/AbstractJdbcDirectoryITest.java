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

import javax.sql.DataSource;

import org.hsqldb.Server;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.server.ServerAcl.AclFormatException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.github.lucene.store.jdbc.datasource.DriverManagerDataSource;
import com.github.lucene.store.jdbc.datasource.TransactionAwareDataSourceProxy;
import com.github.lucene.store.jdbc.dialect.Dialect;
import com.github.lucene.store.jdbc.dialect.DialectResolver;
import com.github.lucene.store.jdbc.support.JdbcTemplate;

/**
 * @author kimchy
 */
public abstract class AbstractJdbcDirectoryITest {

    private static Server server;

    private String dialect;
    protected DataSource dataSource;
    protected JdbcTemplate jdbcTemplate;

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
        final DriverManagerDataSource driverManagerDataSource = new DriverManagerDataSource(driver, url, username,
                password, false);
        dataSource = new TransactionAwareDataSourceProxy(driverManagerDataSource);
        dialect = "com.github.lucene.store.jdbc.dialect.HSQLDialect";
        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    protected Dialect createDialect() throws Exception {
        if (dialect == null) {
            return new DialectResolver().getDialect(dataSource);
        }
        return (Dialect) Class.forName(dialect).newInstance();
    }
}
