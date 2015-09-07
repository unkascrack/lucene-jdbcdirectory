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

    private final String indexTableName;
    private final DataSource dataSource;
    private final Dialect dialect;
    private final LockFactory lockFactory;

    private final DatabaseLuceneHandler handler;

    public DatabaseDirectory(final DataSource dataSource, final Dialect dialect, final String indexTableName)
            throws DatabaseStoreException {
        this(dataSource, dialect, indexTableName, DatabaseLockFactory.INSTANCE);
    }

    public DatabaseDirectory(final DataSource dataSource, final Dialect dialect, final String indexTableName,
            final LockFactory lockFactory) throws DatabaseStoreException {
        this.dataSource = dataSource;
        this.dialect = dialect;
        this.indexTableName = indexTableName;
        this.lockFactory = lockFactory;

        handler = new DatabaseLuceneHandler(this);

        // create directory, if it doesn't exist
        if (dialect.supportsTableExists() && !handler.isIndexTableExists()) {
            LOGGER.debug(this + ": creating lucene index table");
            handler.createIndexTable();
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

    DatabaseLuceneHandler getHandler() {
        return handler;
    }

    @Override
    public String[] listAll() throws IOException {
        LOGGER.debug(this + ".listAll()");
        return handler.listAll();
    }

    @Override
    public void deleteFile(final String name) throws IOException {
        LOGGER.debug(this + ".deleteFile(" + name + ")");
        handler.deleteFile(name);
    }

    @Override
    public long fileLength(final String name) throws IOException {
        LOGGER.debug(this + ".fileLength(" + name + ")");
        return handler.fileLength(name);
    }

    @Override
    public IndexOutput createOutput(final String name, final IOContext context) throws IOException {
        LOGGER.debug(this + ".createOutput(" + name + ", " + context + ")");
        return new DatabaseIndexOutput(name, this);
    }

    @Override
    public void sync(final Collection<String> names) throws IOException {
        LOGGER.debug(this + ".sync(" + names + ")");
        handler.sync(names);
    }

    @Override
    public void renameFile(final String source, final String dest) throws IOException {
        LOGGER.debug(this + ".renameFile(" + source + ", " + dest + ")");
        handler.renameFile(source, dest);
    }

    @Override
    public IndexInput openInput(final String name, final IOContext context) throws IOException {
        LOGGER.debug(this + ".openInput(" + name + ", " + context + ")");
        return new DatabaseIndexInput(name, this, context);
    }

    @Override
    public Lock obtainLock(final String name) throws IOException {
        LOGGER.debug(this + ".obtainLock(" + name + ")");
        return lockFactory.obtainLock(this, name);
    }

    @Override
    public void close() throws IOException {
        // TODO do nothing??
        LOGGER.debug(this + ".close()");
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "@" + indexTableName;
    }
}
