package com.github.lucene.store.jdbc.dialect;

/**
 * An Oracle diaclet. Works with Oracle version 8.
 *
 * @author kimchy
 * @author jbloggs
 */
public class Oracle8Dialect extends Oracle9Dialect {

    @Override
    public String getCurrentTimestampSelectString() {
        return "select sysdate from dual";
    }

    @Override
    public String getCurrentTimestampFunction() {
        return "sysdate";
    }

    @Override
    public String getVarcharType(final int length) {
        return "varchar2(" + length + ")";
    }

    @Override
    public String getTimestampType() {
        return "date";
    }

}
