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

package com.github.lucene.store.database;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IOContext.Context;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.github.lucene.store.AbstractContextIntegrationTests;

public class DatabaseDirectoryITest extends AbstractContextIntegrationTests {

    private Directory directory;

    @Before
    public void initDirectory() throws DatabaseStoreException, IOException {
        directory = new DatabaseDirectory(dataSource, dialect, indexTableName);
    }

    @After
    public void closeDirectory() throws IOException {
        final String[] files = directory.listAll();
        for (final String file : files) {
            directory.deleteFile(file);
        }
        directory.close();
    }

    @Test(expected = IndexNotFoundException.class)
    public void whenIndexIsEmptyAndOpenIndexReader_shouldThrowIndexNotFoundException() throws IOException {
        DirectoryReader.open(directory);
    }

    @Test
    public void list_whenIndexIsEmpty_shouldReturnZeroFiles() throws IOException {
        final String[] files = directory.listAll();
        Assert.assertEquals(0, files.length);
    }

    @Test
    public void list_whenIndexIsNotEmpty_shouldReturnMultipleFiles() throws IOException {
        addContentIndexOutput(directory, "test1", "TEST STRING", Context.FLUSH);
        final String[] files = directory.listAll();
        Assert.assertEquals(1, files.length);
    }

    @Test
    public void fileLength_whenFileNotFound_shouldReturnZero() throws IOException {
        final long length = directory.fileLength("notfound");
        Assert.assertEquals(0l, length);
    }

    @Test
    public void fileLength_whenFileFound_shouldReturnLength() throws IOException {
        addContentIndexOutput(directory, "test1", "TEST STRING", Context.FLUSH);
        final long length = directory.fileLength("test1");
        Assert.assertTrue(length > 0l);
    }

    @Test
    public void deleteFile_whenFileNotFound_shouldNoThrowAnyException() throws IOException {
        directory.deleteFile("notfound");
        Assert.assertEquals(0, directory.listAll().length);
    }

    @Test
    public void deleteFile_whenFileFound_shouldDelete() throws IOException {
        addContentIndexOutput(directory, "test1", "TEST STRING", Context.FLUSH);
        Assert.assertEquals(1, directory.listAll().length);
        directory.deleteFile("test1");
        Assert.assertEquals(0, directory.listAll().length);
    }

    @Test
    public void renameFile_whenFileSourceNotFound_shouldNoThrowAnyException() throws IOException {
        directory.renameFile("notfound", "renamed");
        Assert.assertEquals(0, directory.listAll().length);
    }

    @Test
    public void renameFile_whenFileSourceFound_shouldRenameFile() throws IOException {
        addContentIndexOutput(directory, "test1", "TEST STRING", Context.FLUSH);
        Assert.assertEquals(1, directory.listAll().length);
        Assert.assertEquals("test1", directory.listAll()[0]);
        directory.renameFile("test1", "renamed");
        Assert.assertEquals(1, directory.listAll().length);
        Assert.assertEquals("renamed", directory.listAll()[0]);
    }

    @Test
    public void createOutput_whenIOContextIsFlush_shouldCreateFile() throws IOException {
        addContentIndexOutput(directory, "test1", "TEST STRING", Context.FLUSH);
        Assert.assertEquals(1, directory.listAll().length);
        Assert.assertEquals("test1", directory.listAll()[0]);
    }

    @Test
    public void createOutput_whenIOContextIsNotFlush_shouldNotCreateFile() throws IOException {
        for (final Context context : Context.values()) {
            if (!Context.FLUSH.equals(context)) {
                addContentIndexOutput(directory, "test1", "TEST STRING", context);
            }
        }
        Assert.assertEquals(0, directory.listAll().length);
    }

    @Test(expected = DatabaseStoreException.class)
    public void createOutput_whenFileFound_shouldThrowDatabaseStoreException() throws IOException {
        addContentIndexOutput(directory, "test1", "TEST STRING", Context.FLUSH);
        addContentIndexOutput(directory, "test1", "TEST STRING", Context.FLUSH);
    }

    @Test
    public void createInput_whenFileNotFound_shouldReturnIndexInputWhenZeroLength() throws IOException {
        final IndexInput indexInput = directory.openInput("notfound", new IOContext(Context.READ));
        Assert.assertEquals(0l, indexInput.length());
    }

    @Test
    public void createInput_whenFileFound_shouldReturnIndexInputWhenZeroLength() throws IOException {
        addContentIndexOutput(directory, "test1", "TEST STRING", Context.FLUSH);
        final IndexInput indexInput = directory.openInput("test1", new IOContext(Context.READ));
        Assert.assertNotEquals(0l, indexInput.length());
    }

    @Test
    public void obtainLock_whenLockFileNotFound_shouldReturnLock() throws IOException {
        final Lock lock = directory.obtainLock(IndexWriter.WRITE_LOCK_NAME);
        Assert.assertNotNull(lock);
        Assert.assertTrue(lock instanceof DatabaseLockFactory.DatabaseLock);
        lock.close();
    }

    @Test(expected = LockObtainFailedException.class)
    public void obtainLock_whenLockFileFound_shouldThrowLockObtainFailedException() throws IOException {
        final Lock lock = directory.obtainLock(IndexWriter.WRITE_LOCK_NAME);
        try {
            directory.obtainLock(IndexWriter.WRITE_LOCK_NAME);
        } finally {
            lock.close();
        }
    }

    @Test
    public void obtainLock_whenLockFileFoundButIsClosed_shouldReturnNewLock() throws IOException {
        final Lock lock1 = directory.obtainLock(IndexWriter.WRITE_LOCK_NAME);
        Assert.assertNotNull(lock1);
        lock1.close();

        final Lock lock2 = directory.obtainLock(IndexWriter.WRITE_LOCK_NAME);
        Assert.assertNotNull(lock2);
        lock2.close();
    }

    @Test
    public void sync_shouldNoThrowException() throws IOException {
        directory.sync(Arrays.asList("tt"));
    }

    @Test
    public void close_shouldNoThrowException() throws IOException {
        directory.close();
    }

    private void addContentIndexOutput(final Directory directory, final String fileName, final String content,
            final Context context) throws IOException {
        final IndexOutput indexOutput = directory.createOutput(fileName, new IOContext(context));
        indexOutput.writeString(content);
        indexOutput.close();
    }
}
