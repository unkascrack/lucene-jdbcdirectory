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
 * A H2 dialect.
 *
 * @author kimchy
 */
public class H2Dialect extends Dialect {

    @Override
    public boolean supportsForUpdate() {
        return false;
    }

    @Override
    public String getForUpdateString() {
        return "";
    }

    @Override
    public boolean supportTransactionalScopedBlobs() {
        return true;
    }

    @Override
    public boolean supportsIfExistsAfterTableName() {
        return true;
    }

    @Override
    public String getVarcharType(final int length) {
        return "varchar(" + length + ")";
    }

    @Override
    public String getBlobType(final long length) {
        return "blob";
    }

    @Override
    public String getNumberType() {
        return "integer";
    }

    @Override
    public String getTimestampType() {
        return "timestamp";
    }

    @Override
    public String getCurrentTimestampFunction() {
        return "current_timestamp()";
    }

    @Override
    public String getBitType() {
        return "bit";
    }
}
