package com.github.lucene.store.database;

import java.io.IOException;

public class DatabaseStoreException extends IOException {

    private static final long serialVersionUID = -2446717740777213051L;

    /**
     * @param cause
     */
    DatabaseStoreException(final Throwable cause) {
        super(cause);
    }

    /**
     * @param message
     * @param cause
     */
    DatabaseStoreException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
