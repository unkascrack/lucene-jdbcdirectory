package org.junit.rule;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class RunInThreadRule implements TestRule {

    @Override
    public Statement apply(final Statement base, final Description description) {
        Statement result = base;
        final RunInThread annotation = description.getAnnotation(RunInThread.class);
        if (annotation != null) {
            result = new RunInThreadStatement(base);
        }
        return result;
    }
}
