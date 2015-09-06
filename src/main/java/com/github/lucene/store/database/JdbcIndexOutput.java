package com.github.lucene.store.database;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.lucene.store.IndexOutput;

public class JdbcIndexOutput extends IndexOutput {

    /**
     * At the moment this is the most portable approach at the expense of memory usage spikes.
     */
    private final ByteArrayOutputStream baos;

    private Checksum digest;

    private long pos;

    private final PreparedStatement statement;

    protected JdbcIndexOutput(final String name, final Connection connection, final String searchTableName) {
        super(name);

        try (PreparedStatement lock = connection.prepareStatement(String.format(
                "select name from %1$s where name = ? for update", searchTableName))) {
            lock.setString(1, name);

            try (ResultSet rs = lock.executeQuery()) {
                pos = 0;
                digest = new CRC32();
                baos = new ByteArrayOutputStream();
                if (!rs.next()) {

                    statement = connection.prepareStatement(String.format(
                            "insert into %1$s (content,contentlength, name) values (?, ?, ?)", searchTableName));
                    statement.setString(3, name);
                } else {
                    statement = connection.prepareStatement(String.format(
                            "update %1$s set content = ?, contentlength = ? where name=?", searchTableName));
                    statement.setString(3, name);

                }
            }
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        }

    }

    @Override
    public void close() throws IOException {

        try {

            final byte[] buffer = baos.toByteArray();
            statement.setBinaryStream(1, new ByteArrayInputStream(buffer), buffer.length);
            statement.setLong(2, buffer.length);
            final int c = statement.executeUpdate();
            if (c != 1) {
                throw new PersistenceException("update expected but did not occur");
            }
        } catch (final SQLException e) {
            throw new PersistenceException(e);
        }

    }

    @Override
    public long getChecksum() throws IOException {

        return digest.getValue();
    }

    @Override
    public long getFilePointer() {

        return pos;
    }

    @Override
    public void writeByte(final byte b) throws IOException {

        baos.write(b);
        ++pos;
        digest.update(b);

    }

    @Override
    public void writeBytes(final byte[] buffer, final int offset, final int length) throws IOException {

        baos.write(buffer, offset, length);
        digest.update(buffer, offset, length);
        pos += length;
    }

}
