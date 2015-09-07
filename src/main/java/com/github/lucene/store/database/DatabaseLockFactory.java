package com.github.lucene.store.database;

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

class DatabaseLockFactory extends LockFactory {

    protected static final LockFactory INSTANCE = new DatabaseLockFactory();

    private DatabaseLockFactory() {
    }

    @Override
    public Lock obtainLock(final Directory dir, final String lockName) throws IOException {
        return new DatabaseLock();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }

    static final class DatabaseLock extends Lock {

        @Override
        public void close() throws IOException {
            // TODO Auto-generated method stub

        }

        @Override
        public void ensureValid() throws IOException {
            // TODO Auto-generated method stub

        }
    }

}
