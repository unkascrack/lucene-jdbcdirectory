package com.github.lucene.store.database;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IOContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DatabaseIndexInput extends BufferedIndexInput {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseIndexInput.class);
    private static final DatabaseDirectoryHandler handler = DatabaseDirectoryHandler.INSTANCE;

    private final ByteBuffer buffer;
    private int pos;

    DatabaseIndexInput(final DatabaseDirectory directory, final String name, final IOContext context)
            throws DatabaseStoreException {
        super(name, context);
        final byte[] content = handler.fileContent(directory, name);
        buffer = content != null && content.length > 0 ? ByteBuffer.wrap(content) : ByteBuffer.allocate(0);
        pos = 0;
    }

    @Override
    protected void readInternal(final byte[] b, final int offset, final int length) throws IOException {
        LOGGER.trace("{}.readInternal({}, {}, {})", this, b, offset, length);
        System.arraycopy(buffer.array(), pos, b, offset, length);
    }

    @Override
    protected void seekInternal(final long pos) throws IOException {
        LOGGER.trace("{}.seekInternal({})", this, pos);
        this.pos = (int) pos;
    }

    @Override
    public void close() throws IOException {
        LOGGER.trace("{}.close()", this);
        // TODO Auto-generated method stub
    }

    @Override
    public long length() {
        LOGGER.trace("{}.length()", this);
        return buffer.limit();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }
}
