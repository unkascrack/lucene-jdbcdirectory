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

package com.github.lucene.store.jdbc.support;

import com.github.lucene.store.jdbc.dialect.Dialect;

/**
 * An internal representation of a database column used to store the
 * {@link org.apache.lucene.store.jdbc.JdbcDirectory} settings.
 *
 * @author kimchy
 */
public class JdbcColumn {

    private final Dialect dialect;

    private String name;

    private final int index;

    private final String type;

    private boolean quoted = false;

    public JdbcColumn(final Dialect dialect, final String name, final int index, final String type) {
        this.dialect = dialect;
        setName(name);
        this.index = index;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public int getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public void setName(final String name) {
        if (name.charAt(0) == dialect.openQuote()) {
            quoted = true;
            this.name = name.substring(1, name.length() - 1);
        } else {
            this.name = name;
        }
    }

    public String getQuotedName() {
        return quoted ? dialect.openQuote() + name + dialect.closeQuote() : name;
    }
}
