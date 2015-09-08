package com.github.lucene.store.database;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.lucene.store.IndexOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DatabaseIndexOutput extends IndexOutput {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseIndexOutput.class);

    private final DatabaseDirectory directory;

    private final String name;
    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    private final Checksum digest = new CRC32();
    private long pos = 0;

    DatabaseIndexOutput(final String name, final DatabaseDirectory directory) {
        super(name);
        this.name = name;
        this.directory = directory;
    }

    @Override
    public void close() throws IOException {
        LOGGER.trace("{}.close()", this);
        final byte[] buffer = baos.toByteArray();
        directory.getHandler().save(name, buffer, buffer.length);
    }

    @Override
    public long getFilePointer() {
        LOGGER.trace("{}.getFilePointer()", this);
        return pos;
    }

    @Override
    public long getChecksum() throws IOException {
        LOGGER.trace("{}.getChecksum()", this);
        return digest.getValue();
    }

    @Override
    public void writeByte(final byte b) throws IOException {
        LOGGER.trace("{}.writeByte({})", this, b);
        baos.write(b);
        ++pos;
        digest.update(b);
    }

    @Override
    public void writeBytes(final byte[] b, final int offset, final int length) throws IOException {
        LOGGER.trace("{}.writeBytes({}, {}, {})", this, b, offset, length);
        baos.write(b, offset, length);
        digest.update(b, offset, length);
        pos += length;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }
}
