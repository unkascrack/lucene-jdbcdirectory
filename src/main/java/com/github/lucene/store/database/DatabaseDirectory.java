package com.github.lucene.store.database;

import java.io.IOException;
import java.util.Collection;

import javax.sql.DataSource;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lucene.store.database.dialect.Dialect;

public class DatabaseDirectory extends Directory {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseDirectory.class);

    /**
     *
     */
    private static final DatabaseDirectoryHandler handler = DatabaseDirectoryHandler.INSTANCE;

    private final String indexTableName;
    private final DataSource dataSource;
    private final Dialect dialect;
    private final LockFactory lockFactory;

    /**
     * @param dataSource
     * @param dialect
     * @param indexTableName
     * @throws DatabaseStoreException
     */
    public DatabaseDirectory(final DataSource dataSource, final Dialect dialect, final String indexTableName)
            throws DatabaseStoreException {
        this(dataSource, dialect, indexTableName, DatabaseLockFactory.INSTANCE);
    }

    /**
     * @param dataSource
     * @param dialect
     * @param indexTableName
     * @param lockFactory
     * @throws DatabaseStoreException
     */
    public DatabaseDirectory(final DataSource dataSource, final Dialect dialect, final String indexTableName,
            final LockFactory lockFactory) throws DatabaseStoreException {
        this.dataSource = dataSource;
        this.dialect = dialect;
        this.indexTableName = indexTableName;
        this.lockFactory = lockFactory;

        // create directory, if it doesn't exist
        if (dialect.supportsTableExists() && !handler.existsIndexTable(this)) {
            LOGGER.info("{}: creating lucene index table", this);
            handler.createIndexTable(this);
        }
    }

    public String getIndexTableName() {
        return indexTableName;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public Dialect getDialect() {
        return dialect;
    }

    @Override
    public String[] listAll() throws IOException {
        LOGGER.debug("{}.listAll()", this);
        return handler.listAllFiles(this);
    }

    @Override
    public void deleteFile(final String name) throws IOException {
        LOGGER.debug("{}.deleteFile({})", this, name);
        handler.deleteFile(this, name, true);
    }

    @Override
    public long fileLength(final String name) throws IOException {
        LOGGER.debug("{}.fileLength(name)", this, name);
        return handler.fileLength(this, name);
    }

    @Override
    public IndexOutput createOutput(final String name, final IOContext context) throws IOException {
        LOGGER.debug("{}.createOutput({}, {})", this, name, context);
        return new DatabaseIndexOutput(this, name, context);
    }

    @Override
    public void sync(final Collection<String> names) throws IOException {
        LOGGER.debug("{}.sync({})", this, names);
        handler.syncFiles(this, names);
    }

    @Override
    public void renameFile(final String source, final String dest) throws IOException {
        LOGGER.debug("{}.renameFile({}, {})", this, source, dest);
        handler.renameFile(this, source, dest);
    }

    @Override
    public IndexInput openInput(final String name, final IOContext context) throws IOException {
        LOGGER.debug("{}.openInput({}, {})", this, name, context);
        return new DatabaseIndexInput(this, name, context);
    }

    @Override
    public Lock obtainLock(final String name) throws IOException {
        LOGGER.debug("{}.obtainLock({})", this, name);
        return lockFactory.obtainLock(this, name);
    }

    @Override
    public void close() throws IOException {
        // TODO do nothing??
        LOGGER.debug("{}.close()", this);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "@" + indexTableName;
    }
}
