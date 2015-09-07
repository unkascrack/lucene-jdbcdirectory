package com.github.lucene.store.database;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.Checksum;

import org.apache.lucene.store.IndexOutput;

class DatabaseIndexOutput extends IndexOutput {

    private final DatabaseDirectory directory;

    private final String name;
    private ByteArrayOutputStream baos;
    private Checksum digest;
    private long pos;

    DatabaseIndexOutput(final String name, final DatabaseDirectory directory) {
        super(name);
        this.name = name;
        this.directory = directory;
    }

    @Override
    public void close() throws IOException {
        final byte[] buffer = baos.toByteArray();
        directory.getHandler().save(name, buffer, buffer.length);
    }

    @Override
    public long getFilePointer() {
        return pos;
    }

    @Override
    public long getChecksum() throws IOException {
        return digest.getValue();
    }

    @Override
    public void writeByte(final byte b) throws IOException {
        baos.write(b);
        ++pos;
        digest.update(b);
    }

    @Override
    public void writeBytes(final byte[] b, final int offset, final int length) throws IOException {
        baos.write(b, offset, length);
        digest.update(b, offset, length);
        pos += length;
    }

}
