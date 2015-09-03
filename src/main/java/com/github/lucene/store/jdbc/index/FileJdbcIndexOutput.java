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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lucene.store.jdbc.JdbcDirectory;
import com.github.lucene.store.jdbc.JdbcFileEntrySettings;

/**
 * An <code>IndexOutput</code> implemenation that writes all the data to a
 * temporary file, and when closed, flushes the file to the database.
 * <p/>
 * Usefull for large files that are known in advance to be larger then the
 * acceptable threshold configured in {@link RAMAndFileJdbcIndexOutput}.
 *
 * @author kimchy
 */
public class FileJdbcIndexOutput extends AbstractJdbcIndexOutput {

    private static final Logger logger = LoggerFactory.getLogger(FileJdbcIndexOutput.class);

    private RandomAccessFile file = null;

    private File tempFile;

    public FileJdbcIndexOutput() {
        super("FileJdbcIndexOutput");
    }

    @Override
    public void configure(final String name, final JdbcDirectory jdbcDirectory, final JdbcFileEntrySettings settings)
            throws IOException {
        super.configure(name, jdbcDirectory, settings);
        tempFile = File.createTempFile(
                jdbcDirectory.getTable().getName() + "_" + name + "_" + System.currentTimeMillis(), ".ljt");
        file = new RandomAccessFile(tempFile, "rw");
        this.jdbcDirectory = jdbcDirectory;
        this.name = name;
    }

    @Override
    protected void flushBuffer(final byte[] b, final int offset, final int len) throws IOException {
        file.write(b, offset, len);
    }

    /**
     * Random-access methods
     */
    @Override
    public void seek(final long pos) throws IOException {
        super.seek(pos);
        file.seek(pos);
    }

    @Override
    public long length() throws IOException {
        return file.length();
    }

    @Override
    protected InputStream openInputStream() throws IOException {
        return new BufferedInputStream(new FileInputStream(file.getFD()));
    }

    @Override
    protected void doBeforeClose() throws IOException {
        file.seek(0);
    }

    @Override
    protected void doAfterClose() throws IOException {
        file.close();
        tempFile.delete();
        tempFile = null;
        file = null;
    }

    @Override
    public long getChecksum() throws IOException {
        // TODO Auto-generated method stub
        logger.debug("FileJdbcIndexOutput.getChecksum()");
        return 0;
    }
}
