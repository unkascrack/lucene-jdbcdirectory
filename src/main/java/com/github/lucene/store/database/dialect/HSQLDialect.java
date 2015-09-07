package com.github.lucene.store.database.dialect;

import java.io.IOException;

public class HSQLDialect extends Dialect {

    private static final String DIALECT_CONFIG = "hsqldialect.sql";

    public HSQLDialect() throws IOException {
        super(DIALECT_CONFIG);
    }

    @Override
    public boolean supportsTableExists() {
        return true;
    }

    @Override
    public String sqlTableExists(final String tableName) {
        return super.sqlTableExists(tableName);
    }

}
