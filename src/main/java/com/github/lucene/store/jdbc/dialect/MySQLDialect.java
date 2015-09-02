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
 * A MySQL dialect.
 *
 * @author kimchy
 */
public class MySQLDialect extends Dialect {

    /**
     * MySQL requires quoting the blob column with connector J 3.1 when using
     * emulateLocators=true.
     */
    @Override
    public String openBlobSelectQuote() {
        return "'";
    }

    /**
     * MySQL requires quoting the blob column with connector J 3.1 when using
     * emulateLocators=true.
     */
    @Override
    public String closeBlobSelectQuote() {
        return "'";
    }

    @Override
    public char closeQuote() {
        return '`';
    }

    @Override
    public char openQuote() {
        return '`';
    }

    /**
     * MySQL supports select ... for update.
     */
    @Override
    public boolean supportsForUpdate() {
        return true;
    }

    /**
     * MySQL supports if exists before the table name.
     */
    @Override
    public boolean supportsIfExistsBeforeTableName() {
        return true;
    }

    /**
     * MySQL supports current timestamp selection.
     */
    @Override
    public boolean supportsCurrentTimestampSelection() {
        return true;
    }

    @Override
    public boolean isCurrentTimestampSelectStringCallable() {
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
        return "longblob";
    }

    @Override
    public String getNumberType() {
        return "integer";
    }

    @Override
    public String getTimestampType() {
        return "datetime";
    }

    @Override
    public String getCurrentTimestampFunction() {
        return "current_timestamp";
    }

    @Override
    public String getBitType() {
        return "bit";
    }
}
