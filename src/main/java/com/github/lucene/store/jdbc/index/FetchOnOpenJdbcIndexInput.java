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

package com.github.lucene.store.jdbc.index;

import java.io.IOException;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.lucene.store.IndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lucene.store.jdbc.JdbcDirectory;
import com.github.lucene.store.jdbc.JdbcFileEntrySettings;
import com.github.lucene.store.jdbc.JdbcStoreException;
import com.github.lucene.store.jdbc.support.JdbcTemplate;

/**
 * An <code>IndexInput</code> implementation that will read all the relevant
 * data from the database when created, and will cache it untill it is closed.
 * <p/>
 * Used for small file entries in the database like the segments file.
 *
 * @author kimchy
 */
public class FetchOnOpenJdbcIndexInput extends IndexInput implements JdbcIndexConfigurable {

    private static final Logger logger = LoggerFactory.getLogger(FetchOnOpenJdbcIndexInput.class);

    // There is no synchronizaiton since Lucene RAMDirecoty performs no
    // synchronizations.
    // Need to get to the bottom of it.

    public FetchOnOpenJdbcIndexInput() {
        super("FetchOnOpenJdbcIndexInput");
    }

    private int length;

    private int position = 0;

    private byte[] data;

    @Override
    public void configure(final String name, final JdbcDirectory jdbcDirectory, final JdbcFileEntrySettings settings)
            throws IOException {
        jdbcDirectory.getJdbcTemplate().executeSelect(jdbcDirectory.getTable().sqlSelectSizeValueByName(),
                new JdbcTemplate.ExecuteSelectCallback() {
                    @Override
                    public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                        ps.setFetchSize(1);
                        ps.setString(1, name);
                    }

                    @Override
                    public Object execute(final ResultSet rs) throws Exception {
                        if (!rs.next()) {
                            throw new JdbcStoreException(
                                    "No entry for [" + name + "] table " + jdbcDirectory.getTable());
                        }
                        length = rs.getInt(3);

                        final Blob blob = rs.getBlob(2);
                        data = blob.getBytes(1, length);
                        if (data.length != length) {
                            throw new IOException("read past EOF");
                        }
                        return null;
                    }
                });
    }

    @Override
    public byte readByte() throws IOException {
        return data[position++];
    }

    @Override
    public void readBytes(final byte[] b, final int offset, final int len) throws IOException {
        System.arraycopy(data, position, b, offset, len);
        position += len;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public long getFilePointer() {
        return position;
    }

    @Override
    public void seek(final long pos) throws IOException {
        position = (int) pos;
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public IndexInput slice(final String sliceDescription, final long offset, final long length) throws IOException {
        // TODO Auto-generated method stub
        logger.debug("FetchOnOpenJdbcIndexInput.slice()");
        return null;
    }
}
