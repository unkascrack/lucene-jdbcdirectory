package com.github.lucene.store.database.dialect;

import java.io.IOException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class HSQLDialectTest {

    private static Dialect dialect;
    private static final String TABLENAME = "tableName";

    @BeforeClass
    public static void setUp() throws IOException {
        dialect = new HSQLDialect();
    }

    @Test
    public void supportsTableExists_shouldReturnTrue() {
        Assert.assertTrue(dialect.supportsTableExists());
    }

    @Test
    public void s() {
        final String sql = dialect.sqlSelectAll(TABLENAME);
        Assert.assertNotNull(sql);
        Assert.assertTrue(sql.contains(TABLENAME));
    }
}
