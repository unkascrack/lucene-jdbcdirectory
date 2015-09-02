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

package com.github.lucene.jdbc.store.index;

import java.io.IOException;

import com.github.lucene.jdbc.store.JdbcDirectory;
import com.github.lucene.jdbc.store.JdbcFileEntrySettings;

/**
 * A simple base class that performs index input memory based buffering. The
 * buffer size can be configured under the {@link #BUFFER_SIZE_SETTING} name.
 *
 * @author kimchy
 */
public abstract class JdbcBufferedIndexInput extends ConfigurableBufferedIndexInput implements JdbcIndexConfigurable {

    /**
     * The buffer size setting name. See
     * {@link JdbcFileEntrySettings#setIntSetting(String,int)}. Should be set in
     * bytes.
     */
    public static final String BUFFER_SIZE_SETTING = "indexInput.bufferSize";

    protected JdbcBufferedIndexInput(final String resourceDescription) {
        super(resourceDescription, BUFFER_SIZE);
    }

    @Override
    public void configure(final String name, final JdbcDirectory jdbcDirectory, final JdbcFileEntrySettings settings)
            throws IOException {
        setBufferSize(settings.getSettingAsInt(BUFFER_SIZE_SETTING, BUFFER_SIZE));
    }
}
