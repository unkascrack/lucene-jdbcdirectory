package com.github.lucene.store.database;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IOContext;

class DatabaseIndexInput extends BufferedIndexInput {

    private final ByteBuffer buffer;
    private int pos;

    DatabaseIndexInput(final String name, final DatabaseDirectory directory, final IOContext context)
            throws DatabaseStoreException {
        super(name, context);
        final byte[] content = directory.getHandler().getContent(name);
        buffer = content != null && content.length > 0 ? ByteBuffer.wrap(content) : ByteBuffer.allocate(0);
        pos = 0;
    }

    @Override
    protected void readInternal(final byte[] b, final int offset, final int length) throws IOException {
        System.arraycopy(buffer.array(), pos, b, offset, length);
    }

    @Override
    protected void seekInternal(final long pos) throws IOException {
        this.pos = (int) pos;
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub
    }

    @Override
    public long length() {
        return buffer.limit();
    }

}
