package com.github.lucene.store.database;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;

class DatabaseLuceneHandler {

    private final DatabaseDirectory directory;

    DatabaseLuceneHandler(final DatabaseDirectory directory) {
        this.directory = directory;
    }

    boolean isIndexTableExists() throws DatabaseStoreException {
        final Connection connection = DataSourceUtils.getConnection(directory.getDataSource());
        final String sqlTableExists = directory.getDialect().sqlTableExists(directory.getIndexTableName());
        return (boolean) JdbcTemplate.executeSelect(connection, sqlTableExists,
                new JdbcTemplate.ExecuteSelectCallback() {

                    @Override
                    public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                        // do nothing
                    }

                    @Override
                    public Object execute(final ResultSet rs) throws Exception {
                        return rs.next() ? rs.getBoolean(1) : false;
                    }
                });
    }

    void createIndexTable() throws DatabaseStoreException {
        final Connection connection = DataSourceUtils.getConnection(directory.getDataSource());
        final String sqlCreate = directory.getDialect().sqlTableCreate(directory.getIndexTableName());
        JdbcTemplate.executeUpdate(connection, sqlCreate, new JdbcTemplate.PrepateStatementAwareCallback() {

            @Override
            public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                // do nothing
            }
        });
    }

    public String[] listAll() throws DatabaseStoreException {
        final Connection connection = DataSourceUtils.getConnection(directory.getDataSource());
        final String sqlListAll = directory.getDialect().sqlSelectAll(directory.getIndexTableName());
        return (String[]) JdbcTemplate.executeSelect(connection, sqlListAll, new JdbcTemplate.ExecuteSelectCallback() {

            @Override
            public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
            }

            @Override
            public Object execute(final ResultSet rs) throws Exception {
                final ArrayList<String> names = new ArrayList<String>();
                while (rs.next()) {
                    names.add(rs.getString(1));
                }
                return names.toArray(new String[names.size()]);
            }
        });
    }

    public void deleteFile(final String name) throws DatabaseStoreException {
        final Connection connection = DataSourceUtils.getConnection(directory.getDataSource());
        final String sqlDelete = directory.getDialect().sqlDeleteByName(directory.getIndexTableName());
        JdbcTemplate.executeUpdate(connection, sqlDelete, new JdbcTemplate.PrepateStatementAwareCallback() {

            @Override
            public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                ps.setString(1, name);
            }
        });
    }

    void sync(final Collection<String> names) {
        // TODO Auto-generated method stub

    }

    public long fileLength(final String name) throws DatabaseStoreException {
        final Connection connection = DataSourceUtils.getConnection(directory.getDataSource());
        final String sqlSelectSize = directory.getDialect().sqlSelectSize(directory.getIndexTableName());
        return (long) JdbcTemplate.executeSelect(connection, sqlSelectSize, new JdbcTemplate.ExecuteSelectCallback() {

            @Override
            public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                ps.setString(1, name);
            }

            @Override
            public Object execute(final ResultSet rs) throws Exception {
                return rs.next() ? rs.getLong(2) : null;
            }
        });
    }

    public void renameFile(final String source, final String dest) throws DatabaseStoreException {
        final Connection connection = DataSourceUtils.getConnection(directory.getDataSource());
        final String sqlUpdateRename = directory.getDialect().sqlUpdateRename(directory.getIndexTableName());
        JdbcTemplate.executeUpdate(connection, sqlUpdateRename, new JdbcTemplate.PrepateStatementAwareCallback() {

            @Override
            public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                ps.setString(1, dest);
                ps.setString(2, source);
            }
        });

    }

    public byte[] getContent(final String name) throws DatabaseStoreException {
        final Connection connection = DataSourceUtils.getConnection(directory.getDataSource());
        final String sqlSelectContent = directory.getDialect().sqlSelectContent(directory.getIndexTableName());
        return (byte[]) JdbcTemplate.executeSelect(connection, sqlSelectContent,
                new JdbcTemplate.ExecuteSelectCallback() {

                    @Override
                    public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                        ps.setString(1, name);
                    }

                    @Override
                    public Object execute(final ResultSet rs) throws Exception {
                        return rs.next() ? rs.getBytes(1) : null;
                    }
                });
    }

    public void save(final String name, final byte[] content, final int contentLength) throws DatabaseStoreException {
        final Connection connection1 = DataSourceUtils.getConnection(directory.getDataSource());
        final String sqlSelectName = directory.getDialect().sqlSelectName(directory.getIndexTableName());
        final boolean exists = (boolean) JdbcTemplate.executeSelect(connection1, sqlSelectName,
                new JdbcTemplate.ExecuteSelectCallback() {

                    @Override
                    public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                        ps.setString(1, name);
                    }

                    @Override
                    public Object execute(final ResultSet rs) throws Exception {
                        return rs.next() ? Boolean.TRUE : Boolean.FALSE;
                    }
                });
        String sqlSave = null;
        if (!exists) {
            sqlSave = directory.getDialect().sqlInsert(directory.getIndexTableName());
        } else {
            sqlSave = directory.getDialect().sqlUpdate(directory.getIndexTableName());
        }

        final Connection connection2 = DataSourceUtils.getConnection(directory.getDataSource());
        JdbcTemplate.executeUpdate(connection2, sqlSave, new JdbcTemplate.PrepateStatementAwareCallback() {

            @Override
            public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                ps.setBinaryStream(1, new ByteArrayInputStream(content), contentLength);
                ps.setInt(2, contentLength);
                ps.setString(3, name);
            }
        });
    }
}
