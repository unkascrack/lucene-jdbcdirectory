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
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.github.lucene.jdbc.store.JdbcDirectory;
import com.github.lucene.jdbc.store.JdbcFileEntrySettings;
import com.github.lucene.jdbc.store.JdbcStoreException;
import com.github.lucene.jdbc.store.support.JdbcTemplate;

/**
 * An <code>IndexInput</code> implementation, that for every buffer refill will go and fetch the data from the database.
 *
 * @author kimchy
 */
public class FetchOnBufferReadJdbcIndexInput extends JdbcBufferedIndexInput {

    private String name;

    // lazy intialize the length
    private long totalLength = -1;

    private long position = 1;

    private JdbcDirectory jdbcDirectory;

    public void configure(final String name, final JdbcDirectory jdbcDirectory, final JdbcFileEntrySettings settings)
            throws IOException {
        super.configure(name, jdbcDirectory, settings);
        this.jdbcDirectory = jdbcDirectory;
        this.name = name;
    }

    // Overriding refill here since we can execute a single query to get both the length and the buffer data
    // resulted in not the nicest OO design, where the buffer information is protected in the JdbcBufferedIndexInput
    // class
    // and code duplication between this method and JdbcBufferedIndexInput.
    // Performance is much better this way!
    @Override
    protected void refill() throws IOException {
        jdbcDirectory.getJdbcTemplate().executeSelect(jdbcDirectory.getTable().sqlSelectSizeValueByName(),
                new JdbcTemplate.ExecuteSelectCallback() {
            @Override
            public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                ps.setFetchSize(1);
                ps.setString(1, name);
            }

            @Override
            public Object execute(final ResultSet rs) throws Exception {
                // START read blob and update length if required
                if (!rs.next()) {
                    throw new JdbcStoreException("No entry for [" + name + "] table "
                                    + jdbcDirectory.getTable());
                }
                synchronized (this) {
                    if (totalLength == -1) {
                        totalLength = rs.getLong(3);
                    }
                }
                // END read blob and update length if required

                final long start = bufferStart + bufferPosition;
                long end = start + bufferSize;
                if (end > length()) {
                    end = length();
                }
                bufferLength = (int) (end - start);
                if (bufferLength <= 0) {
                    throw new IOException("read past EOF");
                }

                if (buffer == null) {
                    buffer = new byte[bufferSize]; // allocate buffer lazily
                    seekInternal(bufferStart);
                }
                // START replace read internal
                final Blob blob = rs.getBlob(2);
                readInternal(blob, buffer, 0, bufferLength);

                bufferStart = start;
                bufferPosition = 0;
                return null;
            }
        });
    }

    @Override
    protected synchronized void readInternal(final byte[] b, final int offset, final int length) throws IOException {
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
                    throw new JdbcStoreException("No entry for [" + name + "] table "
                                    + jdbcDirectory.getTable());
                }
                final Blob blob = rs.getBlob(2);
                readInternal(blob, b, offset, length);
                synchronized (this) {
                    if (totalLength == -1) {
                        totalLength = rs.getLong(3);
                    }
                }
                return null;
            }
        });
    }

    /**
     * A helper methods that already reads an open blob
     */
    private synchronized void readInternal(final Blob blob, final byte[] b, final int offset, final int length)
            throws Exception {
        final long curPos = getFilePointer();
        if (curPos + 1 != position) {
            position = curPos + 1;
        }
        final byte[] bytesRead = blob.getBytes(position, length);
        if (bytesRead.length != length) {
            throw new IOException("read past EOF");
        }
        System.arraycopy(bytesRead, 0, b, offset, length);
        position += bytesRead.length;
    }

    @Override
    protected void seekInternal(final long pos) throws IOException {
        position = pos + 1;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public synchronized long length() {
        if (totalLength == -1) {
            try {
                totalLength = jdbcDirectory.fileLength(name);
            } catch (final IOException e) {
                // do nothing here for now, much better for performance
            }
        }
        return totalLength;
    }
}
