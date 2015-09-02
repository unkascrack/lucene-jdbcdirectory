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
 * An Oracle dialect. Works for Oracle 9 and above.
 *
 * @author kimchy
 */
public class OracleDialect extends Dialect {

    /**
     * Oracle supports transactional scopes blobs.
     */
    @Override
    public boolean supportTransactionalScopedBlobs() {
        return true;
    }

    /**
     * Oracle supports select ... for update.
     */
    @Override
    public boolean supportsForUpdate() {
        return true;
    }

    /**
     * Oracle supports if table exists queries.
     */
    @Override
    public boolean supportsTableExists() {
        return true;
    }

    @Override
    public String sqlTableExists(final String catalog, final String schemaName) {
        if (schemaName == null) {
            return "select table_name from user_tables where lower(table_name) = ?";
        }
        return "select table_name from all_tables where lower(owner) = '" + schemaName.toLowerCase()
                + "' and lower(table_name) = ?";
    }

    @Override
    public String getCascadeConstraintsString() {
        return " cascade constraints";
    }

    @Override
    public String getForUpdateNowaitString() {
        return " for update nowait";
    }

    @Override
    public boolean supportsCurrentTimestampSelection() {
        return true;
    }

    @Override
    public String getCurrentTimestampSelectString() {
        return "select systimestamp from dual";
    }

    @Override
    public boolean isCurrentTimestampSelectStringCallable() {
        return false;
    }

    @Override
    public String getVarcharType(final int length) {
        return "varchar2(" + length + " char)";
    }

    @Override
    public String getBlobType(final long length) {
        return "blob";
    }

    @Override
    public String getNumberType() {
        return "number(10,0)";
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
        return "number(1,0)";
    }
}
