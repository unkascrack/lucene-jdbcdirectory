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

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import javax.sql.DataSource;

import com.github.lucene.store.jdbc.datasource.DriverManagerDataSource;
import com.github.lucene.store.jdbc.datasource.TransactionAwareDataSourceProxy;
import com.github.lucene.store.jdbc.dialect.Dialect;
import com.github.lucene.store.jdbc.dialect.DialectResolver;
import com.github.lucene.store.jdbc.support.JdbcTemplate;

import junit.framework.TestCase;

/**
 * @author kimchy
 */
public abstract class AbstractJdbcDirectoryITest extends TestCase {

    private String dialect;

    protected DataSource dataSource;

    protected JdbcTemplate jdbcTemplate;

    @Override
    protected void setUp() throws Exception {
        final File testPropsFile = new File("compass.test.properties");
        final Properties testProps = new Properties();
        if (testPropsFile.exists()) {
            testProps.load(new FileInputStream(testPropsFile));
        }
        String url = testProps.getProperty("compass.engine.connection");
        if (url == null || !url.startsWith("jdbc://")) {
            url = "jdbc:hsqldb:mem:test";
        } else {
            url = url.substring("jdbc://".length());
        }
        final String driver = testProps.getProperty("compass.engine.store.jdbc.connection.driverClass",
                "org.hsqldb.jdbcDriver");
        final String username = testProps.getProperty("compass.engine.store.jdbc.connection.username", "sa");
        final String password = testProps.getProperty("compass.engine.store.jdbc.connection.password", "");
        final DriverManagerDataSource driverManagerDataSource = new DriverManagerDataSource(driver, url, username,
                password, false);
        dataSource = new TransactionAwareDataSourceProxy(driverManagerDataSource);
        dialect = testProps.getProperty("compass.engine.store.jdbc.dialect");
        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    protected void tearDown() throws Exception {
    }

    protected Dialect createDialect() throws Exception {
        if (dialect == null) {
            return new DialectResolver().getDialect(dataSource);
        }
        return (Dialect) Class.forName(dialect).newInstance();
    }
}
