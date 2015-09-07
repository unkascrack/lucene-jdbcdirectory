package com.github.lucene.store.database.dialect;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public abstract class Dialect {

    private static final String DEFAULT_CONFIG = "dialect.sql";

    protected static final String PROPERTY_SQL_TABLE_EXISTS = "sql.table.exists";
    protected static final String PROPERTY_SQL_TABLE_CREATE = "sql.table.create";
    protected static final String PROPERTY_SQL_SELECT_ALL = "sql.select.listall";
    protected static final String PROPERTY_SQL_SELECT_NAME = "sql.select.name";
    protected static final String PROPERTY_SQL_SELECT_SIZE = "sql.select.size";
    protected static final String PROPERTY_SQL_SELECT_CONTENT = "sql.select.content";
    protected static final String PROPERTY_SQL_INSERT = "sql.insert";
    protected static final String PROPERTY_SQL_UPDATE = "sql.insert";
    protected static final String PROPERTY_SQL_UPDATE_RENAME = "sql.update.rename";
    protected static final String PROPERTY_SQL_DELETE = "sql.delete";

    private final Properties properties;

    public Dialect(final String dialectConfig) throws IOException {
        properties = new Properties();
        properties.load(getClass().getResourceAsStream(DEFAULT_CONFIG));
        InputStream stream = getClass().getResourceAsStream(dialectConfig);
        if (stream == null) {
            stream = getClass().getClassLoader().getResourceAsStream(dialectConfig);
        }
        properties.load(stream);
    }

    /**
     * Does the dialect support a special query to check if a table exists.
     * Defaults to <code>false</code>.
     *
     * @return
     */
    public boolean supportsTableExists() {
        return false;
    }

    /**
     * If the dialect support a special query to check if a table exists, the
     * actual sql that is used to perform it. Defaults to throw an Unsupported
     * excetion (see {@link #supportsTableExists()}.
     *
     * @param tableName
     * @param schemaName
     * @return
     */
    public String sqlTableExists(final String tableName) {
        throw new UnsupportedOperationException("Not sql provided to define if a table exists");
    }

    /**
     * @param tableName
     * @return
     */
    public String sqlTableCreate(final String tableName) {
        return String.format(properties.getProperty(PROPERTY_SQL_TABLE_CREATE), tableName);
    }

    /**
     * @param tableName
     * @return
     */
    public String sqlSelectAll(final String tableName) {
        return String.format(properties.getProperty(PROPERTY_SQL_SELECT_ALL), tableName);
    }

    /**
     * @param tableName
     * @return
     */
    public String sqlSelectName(final String tableName) {
        return String.format(properties.getProperty(PROPERTY_SQL_SELECT_NAME), tableName);
    }

    /**
     * @param tableName
     * @return
     */
    public String sqlSelectSize(final String tableName) {
        return String.format(properties.getProperty(PROPERTY_SQL_SELECT_SIZE), tableName);
    }

    /**
     * @param tableName
     * @return
     */
    public String sqlSelectContent(final String tableName) {
        return String.format(properties.getProperty(PROPERTY_SQL_SELECT_CONTENT), tableName);
    }

    /**
     * @param tableName
     * @return
     */
    public String sqlInsert(final String tableName) {
        return String.format(properties.getProperty(PROPERTY_SQL_INSERT), tableName);
    }

    /**
     * @param tableName
     * @return
     */
    public String sqlUpdate(final String tableName) {
        return String.format(properties.getProperty(PROPERTY_SQL_UPDATE), tableName);
    }

    /**
     * @param tableName
     * @return
     */
    public String sqlUpdateRename(final String tableName) {
        return String.format(properties.getProperty(PROPERTY_SQL_UPDATE_RENAME), tableName);
    }

    /**
     * @param tableName
     * @return
     */
    public String sqlDeleteByName(final String tableName) {
        return String.format(properties.getProperty(PROPERTY_SQL_DELETE), tableName);
    }
}
