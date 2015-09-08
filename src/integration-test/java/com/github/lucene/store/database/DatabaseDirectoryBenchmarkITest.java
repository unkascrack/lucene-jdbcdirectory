package com.github.lucene.store.database;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.Collection;

import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Before;
import org.junit.Test;

import com.github.lucene.store.AbstractContextIntegrationTests;

public class DatabaseDirectoryBenchmarkITest extends AbstractContextIntegrationTests {

    private Directory fsDirectory;
    private Directory ramDirectory;
    private Directory databaseDirectory;

    private final Collection<String> docs = loadDocuments(3000, 5);
    private final OpenMode openMode = OpenMode.CREATE_OR_APPEND;
    private final boolean useCompoundFile = false;

    @Before
    public void setUp() throws Exception {
        ramDirectory = new RAMDirectory();
        fsDirectory = FSDirectory.open(FileSystems.getDefault().getPath("target/index"));
        databaseDirectory = new DatabaseDirectory(dataSource, dialect, indexTableName);
    }

    @Test
    public void testTiming() throws IOException {
        final long ramTiming = timeIndexWriter(ramDirectory);
        final long fsTiming = timeIndexWriter(fsDirectory);
        final long databaseTiming = timeIndexWriter(databaseDirectory);

        System.out.println("RAMDirectory Time: " + ramTiming + " ms");
        System.out.println("FSDirectory Time : " + fsTiming + " ms");
        System.out.println("DatabaseDirectory Time : " + databaseTiming + " ms");
    }

    private long timeIndexWriter(final Directory dir) throws IOException {
        final long start = System.currentTimeMillis();
        addDocuments(dir, openMode, useCompoundFile, docs);
        addDocuments(dir, openMode, useCompoundFile, docs);
        optimize(dir, openMode, useCompoundFile);
        final long stop = System.currentTimeMillis();
        return stop - start;
    }
}
