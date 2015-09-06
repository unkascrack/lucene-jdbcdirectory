package com.github.lucene.store.database;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

@Singleton
@Startup
public class SingletonLockFactory extends LockFactory {

    private class SingleLock extends Lock {

        private final String lockName;

        public SingleLock(final String lockName) {
            this.lockName = lockName;
            locks.putIfAbsent(lockName, false);
        }

        @Override
        public void close() throws IOException {

            locks.replace(lockName, true, false);

        }

        @Override
        public boolean isLocked() throws IOException {

            return locks.get(lockName);
        }

        @Override
        public boolean obtain() throws IOException {

            return locks.replace(lockName, false, true);
        }

    }

    private final ConcurrentMap<String, Boolean> locks = new ConcurrentHashMap<>();

    @Override
    public Lock makeLock(final Directory dir, final String lockName) {

        return new SingleLock(lockName);
    }

}
