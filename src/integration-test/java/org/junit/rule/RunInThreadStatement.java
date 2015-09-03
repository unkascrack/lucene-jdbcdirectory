package org.junit.rule;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.runners.model.Statement;

public class RunInThreadStatement extends Statement {

    private final Statement baseStatement;
    private Future<?> future;
    private volatile Throwable throwable;

    RunInThreadStatement(final Statement baseStatement) {
        this.baseStatement = baseStatement;
    }

    @Override
    public void evaluate() throws Throwable {
        final ExecutorService executorService = runInThread();
        try {
            waitTillFinished();
        } finally {
            executorService.shutdown();
        }
        rethrowAssertionsAndErrors();
    }

    private ExecutorService runInThread() {
        final ExecutorService result = Executors.newSingleThreadExecutor();
        future = result.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    baseStatement.evaluate();
                } catch (final Throwable throwable) {
                    RunInThreadStatement.this.throwable = throwable;
                }
            }
        });
        return result;
    }

    private void waitTillFinished() {
        try {
            future.get();
        } catch (final ExecutionException shouldNotHappen) {
            throw new IllegalStateException(shouldNotHappen);
        } catch (final InterruptedException shouldNotHappen) {
            throw new IllegalStateException(shouldNotHappen);
        }
    }

    private void rethrowAssertionsAndErrors() throws Throwable {
        if (throwable != null) {
            throw throwable;
        }
    }
}