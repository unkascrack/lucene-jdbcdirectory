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

package com.github.lucene.store.jdbc.dialect;

/**
 * A PostgreSQL dialect.
 *
 * @author kimchy
 */
public class PostgreSQLDialect extends Dialect {

    /**
     * PostreSQL supports select ... for update.
     */
    @Override
    public boolean supportsForUpdate() {
        return true;
    }

    /**
     * PostgreSQL supports transactional scoped blobs.
     */
    @Override
    public boolean supportTransactionalScopedBlobs() {
        return true;
    }

    /**
     * PostrgreSQL supports a table exists query.
     */
    @Override
    public boolean supportsTableExists() {
        return true;
    }

    @Override
    public String sqlTableExists(final String catalog, String schemaName) {
        if (schemaName == null || schemaName.length() == 0) {
            schemaName = "public";
        }
        return "select tablename from pg_tables where schemaname = '" + schemaName + "' and lower(tablename) = ?";
    }

    @Override
    public boolean supportsCurrentTimestampSelection() {
        return true;
    }

    @Override
    public boolean isCurrentTimestampSelectStringCallable() {
        return false;
    }

    @Override
    public boolean useInputStreamToInsertBlob() {
        return false;
    }

    @Override
    public String getCurrentTimestampSelectString() {
        return "select now()";
    }

    @Override
    public String getVarcharType(final int length) {
        return "varchar(" + length + ")";
    }

    @Override
    public String getBlobType(final long length) {
        return "oid";
    }

    @Override
    public String getNumberType() {
        return "int4";
    }

    @Override
    public String getTimestampType() {
        return "timestamp";
    }

    @Override
    public String getCurrentTimestampFunction() {
        return "current_timestamp";
    }

    @Override
    public String getBitType() {
        return "bool";
    }
}
