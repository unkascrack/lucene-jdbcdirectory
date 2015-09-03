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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;

import org.apache.lucene.store.IndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lucene.store.jdbc.JdbcDirectory;
import com.github.lucene.store.jdbc.JdbcFileEntrySettings;
import com.github.lucene.store.jdbc.JdbcStoreException;
import com.github.lucene.store.jdbc.datasource.DataSourceUtils;
import com.github.lucene.store.jdbc.support.JdbcTable;

/**
 * Caches blobs per transaction. Only supported for dialects that supports blobs
 * per transaction (see
 * {@link org.apache.lucene.store.jdbc.dialect.Dialect#supportTransactionalScopedBlobs()}
 * .
 * <p/>
 * Note, using this index input requires calling the
 * {@link #releaseBlobs(java.sql.Connection)} when the transaction ends. It is
 * automatically taken care of if using
 * {@link org.apache.lucene.store.jdbc.datasource.TransactionAwareDataSourceProxy}
 * . If using JTA for example, a transcation synchronization should be
 * registered with JTA to clear the blobs.
 *
 * @author kimchy
 */
public class FetchPerTransactionJdbcIndexInput extends JdbcBufferedIndexInput {

    private static final Logger logger = LoggerFactory.getLogger(FetchPerTransactionJdbcIndexInput.class);

    private static final Object blobHolderLock = new Object();

    private static final ThreadLocal<HashMap<Object, HashMap<String, Blob>>> blobHolder = new ThreadLocal<HashMap<Object, HashMap<String, Blob>>>();

    public FetchPerTransactionJdbcIndexInput() {
        super("FetchPerTransactionJdbcIndexInput");
    }

    public static void releaseBlobs(final Connection connection) {
        synchronized (blobHolderLock) {
            final Connection targetConnection = DataSourceUtils.getTargetConnection(connection);
            final HashMap<Object, HashMap<String, Blob>> holdersPerConn = blobHolder.get();
            if (holdersPerConn == null) {
                return;
            }
            holdersPerConn.remove(targetConnection);
            holdersPerConn.remove(new Integer(System.identityHashCode(targetConnection)));
            if (holdersPerConn.isEmpty()) {
                blobHolder.set(null);
            }
        }
    }

    public static void releaseBlobs(final Connection connection, final JdbcTable table, final String name) {
        synchronized (blobHolderLock) {
            final Connection targetConnection = DataSourceUtils.getTargetConnection(connection);
            final HashMap<Object, HashMap<String, Blob>> holdersPerConn = blobHolder.get();
            if (holdersPerConn == null) {
                return;
            }
            HashMap<String, Blob> holdersPerName = holdersPerConn.get(targetConnection);
            if (holdersPerName != null) {
                holdersPerName.remove(name);
            }
            holdersPerName = holdersPerConn.get(new Integer(System.identityHashCode(targetConnection)));
            if (holdersPerName != null) {
                holdersPerName.remove(table.getName() + name);
            }
        }
    }

    private static Blob getBoundBlob(final Connection connection, final JdbcTable table, final String name) {
        synchronized (blobHolderLock) {
            final Connection targetConnection = DataSourceUtils.getTargetConnection(connection);
            final HashMap<Object, HashMap<String, Blob>> holdersPerConn = blobHolder.get();
            if (holdersPerConn == null) {
                return null;
            }
            HashMap<String, Blob> holdersPerName = holdersPerConn.get(targetConnection);
            if (holdersPerName == null) {
                holdersPerName = holdersPerConn.get(new Integer(System.identityHashCode(targetConnection)));
                if (holdersPerName == null) {
                    return null;
                }
            }
            final Blob blob = holdersPerName.get(table.getName() + name);
            if (blob != null) {
                return blob;
            }
            return null;
        }
    }

    private static void bindBlob(final Connection connection, final JdbcTable table, final String name,
            final Blob blob) {
        synchronized (blobHolderLock) {
            final Connection targetConnection = DataSourceUtils.getTargetConnection(connection);
            HashMap<Object, HashMap<String, Blob>> holdersPerCon = blobHolder.get();
            if (holdersPerCon == null) {
                holdersPerCon = new HashMap<Object, HashMap<String, Blob>>();
                blobHolder.set(holdersPerCon);
            }
            HashMap<String, Blob> holdersPerName = holdersPerCon.get(targetConnection);
            if (holdersPerName == null) {
                holdersPerName = holdersPerCon.get(new Integer(System.identityHashCode(targetConnection)));
                if (holdersPerName == null) {
                    holdersPerName = new HashMap<String, Blob>();
                    holdersPerCon.put(targetConnection, holdersPerName);
                    holdersPerCon.put(new Integer(System.identityHashCode(targetConnection)), holdersPerName);
                }
            }

            holdersPerName.put(table.getName() + name, blob);
        }
    }

    private String name;

    // lazy intialize the length
    private long totalLength = -1;

    private long position = 1;

    private JdbcDirectory jdbcDirectory;

    @Override
    public void configure(final String name, final JdbcDirectory jdbcDirectory, final JdbcFileEntrySettings settings)
            throws IOException {
        super.configure(name, jdbcDirectory, settings);
        this.jdbcDirectory = jdbcDirectory;
        this.name = name;
    }

    // Overriding refill here since we can execute a single query to get both
    // the length and the buffer data
    // resulted in not the nicest OO design, where the buffer information is
    // protected in the JdbcBufferedIndexInput class
    // and code duplication between this method and JdbcBufferedIndexInput.
    // Performance is much better this way!
    @Override
    protected void refill() throws IOException {
        final Connection conn = DataSourceUtils.getConnection(jdbcDirectory.getDataSource());
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            Blob blob = getBoundBlob(conn, jdbcDirectory.getTable(), name);
            if (blob == null) {
                ps = conn.prepareStatement(jdbcDirectory.getTable().sqlSelectSizeValueByName());
                ps.setFetchSize(1);
                ps.setString(1, name);

                rs = ps.executeQuery();

                // START read blob and update length if required
                if (!rs.next()) {
                    throw new JdbcStoreException("No entry for [" + name + "] table " + jdbcDirectory.getTable());
                }
                synchronized (this) {
                    if (totalLength == -1) {
                        totalLength = rs.getLong(3);
                    }
                }
                // END read blob and update length if required

                blob = rs.getBlob(2);
                bindBlob(conn, jdbcDirectory.getTable(), name, blob);
            } else {
            }

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
            // readInternal(buffer, 0, bufferLength);
            readInternal(blob, buffer, 0, bufferLength);

            bufferStart = start;
            bufferPosition = 0;
        } catch (final Exception e) {
            throw new JdbcStoreException("Failed to read transactional blob [" + name + "]", e);
        } finally {
            DataSourceUtils.closeResultSet(rs);
            DataSourceUtils.closeStatement(ps);
            DataSourceUtils.releaseConnection(conn);
        }
    }

    @Override
    protected synchronized void readInternal(final byte[] b, final int offset, final int length) throws IOException {
        final Connection conn = DataSourceUtils.getConnection(jdbcDirectory.getDataSource());
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            Blob blob = getBoundBlob(conn, jdbcDirectory.getTable(), name);
            if (blob == null) {
                ps = conn.prepareStatement(jdbcDirectory.getTable().sqlSelectSizeValueByName());
                ps.setFetchSize(1);
                ps.setString(1, name);

                rs = ps.executeQuery();

                if (!rs.next()) {
                    throw new JdbcStoreException("No entry for [" + name + "] table " + jdbcDirectory.getTable());
                }

                blob = rs.getBlob(2);
                bindBlob(conn, jdbcDirectory.getTable(), name, blob);

                synchronized (this) {
                    if (totalLength == -1) {
                        totalLength = rs.getLong(3);
                    }
                }
            }
            readInternal(blob, b, offset, length);
        } catch (final Exception e) {
            throw new JdbcStoreException("Failed to read transactional blob [" + name + "]", e);
        } finally {
            DataSourceUtils.closeResultSet(rs);
            DataSourceUtils.closeStatement(ps);
            DataSourceUtils.releaseConnection(conn);
        }
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
        if (position + length > length() + 1) {
            System.err.println("BAD");
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
        final Connection conn = DataSourceUtils.getConnection(jdbcDirectory.getDataSource());
        try {
            releaseBlobs(conn, jdbcDirectory.getTable(), name);
        } finally {
            DataSourceUtils.releaseConnection(conn);
        }
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

    @Override
    public IndexInput slice(final String sliceDescription, final long offset, final long length) throws IOException {
        // TODO Auto-generated method stub
        logger.debug("FetchPerTransactionJdbcIndexInput.slice()");
        return null;
    }

}
