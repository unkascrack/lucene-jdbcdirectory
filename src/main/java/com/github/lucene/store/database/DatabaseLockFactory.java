package com.github.lucene.store.database;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Types;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.LockObtainFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseLockFactory extends LockFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseLockFactory.class);

    public static final LockFactory INSTANCE = new DatabaseLockFactory();

    private DatabaseLockFactory() {
    }

    @Override
    public Lock obtainLock(final Directory dir, final String lockName) throws IOException {
        LOGGER.info("{}.obtainLock({}, {})", this, dir, lockName);

        final DatabaseDirectory directory = (DatabaseDirectory) dir;
        final Connection connection = DataSourceUtils.getConnection(directory.getDataSource());
        final String sqlInsert = directory.getDialect().sqlInsert(directory.getIndexTableName());
        try {
            JdbcTemplate.executeUpdate(connection, sqlInsert, true, new JdbcTemplate.PrepateStatementAwareCallback() {

                @Override
                public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                    ps.setFetchSize(1);
                    ps.setNull(1, Types.BLOB);
                    ps.setInt(2, 0);
                    ps.setString(3, lockName);
                }
            });
            return new DatabaseLock(directory, lockName);
        } catch (final DatabaseStoreException e) {
            throw new LockObtainFailedException("Lock instance already obtained: " + directory);
        }
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }

    static final class DatabaseLock extends Lock {

        private final DatabaseDirectory directory;
        private final String name;
        private volatile boolean closed;

        public DatabaseLock(final DatabaseDirectory directory, final String name) {
            this.directory = directory;
            this.name = name;
        }

        @Override
        public void ensureValid() throws IOException {
            LOGGER.debug("{}.ensureValid()", this);
            if (closed) {
                throw new AlreadyClosedException("Lock instance already released: " + this);
            }
            if (!directory.getHandler().existsFile(name)) {
                throw new AlreadyClosedException("Lock instance already released: " + this);
            }
        }

        @Override
        public void close() throws IOException {
            LOGGER.debug("{}.close()", this);
            if (!closed) {
                final Connection connection = DataSourceUtils.getConnection(directory.getDataSource());
                final String sqlDelete = directory.getDialect().sqlDeleteByName(directory.getIndexTableName());
                JdbcTemplate.executeUpdate(connection, sqlDelete, true,
                        new JdbcTemplate.PrepateStatementAwareCallback() {

                            @Override
                            public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                                ps.setFetchSize(1);
                                ps.setString(1, name);
                            }
                        });
                closed = true;
            }
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName();
        }
    }

}
