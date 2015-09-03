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
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.IndexOutput;

import com.github.lucene.store.jdbc.JdbcDirectory;
import com.github.lucene.store.jdbc.JdbcFileEntrySettings;

/**
 * An <code>IndexOutput</code> implementation that initially writes the data to
 * a memory buffer. Once it exceeds the configured threshold (
 * {@link #INDEX_OUTPUT_THRESHOLD_SETTING}, will start working with a temporary
 * file, releasing the previous buffer.
 *
 * @author kimchy
 */
public class RAMAndFileJdbcIndexOutput extends IndexOutput implements JdbcIndexConfigurable {

    /**
     * The threshold setting name. See
     * {@link JdbcFileEntrySettings#setLongSetting(String, long)}. Should be set
     * in bytes.
     */
    public static final String INDEX_OUTPUT_THRESHOLD_SETTING = "indexOutput.threshold";

    /**
     * The default value for the threshold (in bytes). Currently 16K.
     */
    public static final long DEFAULT_THRESHOLD = 16 * 1024;

    private long threshold;

    private RAMJdbcIndexOutput ramIndexOutput;

    private FileJdbcIndexOutput fileIndexOutput;

    private JdbcDirectory jdbcDirectory;

    private String name;

    private JdbcFileEntrySettings settings;

    private long position;

    private final Checksum crc;

    public RAMAndFileJdbcIndexOutput() {
        super("RAMAndFileJdbcIndexOutput");
        crc = new BufferedChecksum(new CRC32());
    }

    @Override
    public void configure(final String name, final JdbcDirectory jdbcDirectory, final JdbcFileEntrySettings settings)
            throws IOException {
        this.jdbcDirectory = jdbcDirectory;
        this.name = name;
        this.settings = settings;
        threshold = settings.getSettingAsLong(INDEX_OUTPUT_THRESHOLD_SETTING, DEFAULT_THRESHOLD);
        ramIndexOutput = createRamJdbcIndexOutput();
        ramIndexOutput.configure(name, jdbcDirectory, settings);
    }

    @Override
    public void writeByte(final byte b) throws IOException {
        switchIfNeeded(1).writeByte(b);
        crc.update(b);
    }

    @Override
    public void writeBytes(final byte[] b, final int offset, final int length) throws IOException {
        switchIfNeeded(length).writeBytes(b, offset, length);
        crc.update(b, offset, length);
    }

    @Override
    public void close() throws IOException {
        actualOutput().close();
    }

    @Override
    public long getFilePointer() {
        return actualOutput().getFilePointer();
    }

    @Override
    public long getChecksum() throws IOException {
        return crc.getValue();
    }

    private IndexOutput actualOutput() {
        if (fileIndexOutput != null) {
            return fileIndexOutput;
        }
        return ramIndexOutput;
    }

    private IndexOutput switchIfNeeded(final int length) throws IOException {
        if (fileIndexOutput != null) {
            return fileIndexOutput;
        }
        position += length;
        if (position < threshold) {
            return ramIndexOutput;
        }
        fileIndexOutput = createFileJdbcIndexOutput();
        fileIndexOutput.configure(name, jdbcDirectory, settings);
        ramIndexOutput.flushToIndexOutput(fileIndexOutput);
        // let it be garbage collected
        ramIndexOutput = null;

        return fileIndexOutput;
    }

    protected FileJdbcIndexOutput createFileJdbcIndexOutput() {
        return new FileJdbcIndexOutput();
    }

    protected RAMJdbcIndexOutput createRamJdbcIndexOutput() {
        return new RAMJdbcIndexOutput();
    }

}
