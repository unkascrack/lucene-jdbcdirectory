package com.github.lucene.store.database;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;

class DatabaseDirectoryHandler {

    static final DatabaseDirectoryHandler INSTANCE = new DatabaseDirectoryHandler();

    private DatabaseDirectoryHandler() {
    }

    /**
     * @param directory
     * @return
     * @throws DatabaseStoreException
     */
    boolean existsIndexTable(final DatabaseDirectory directory) throws DatabaseStoreException {
        final Connection connection = DataSourceUtils.getConnection(directory.getDataSource());
        final String sqlTableExists = directory.getDialect().sqlTableExists(directory.getIndexTableName());
        boolean exists = false;
        try {
            exists = (boolean) JdbcTemplate.executeSelect(connection, sqlTableExists,
                    new JdbcTemplate.ExecuteSelectCallback() {

                        @Override
                        public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                            // do nothing
                        }

                        @Override
                        public Object execute(final ResultSet rs) throws Exception {
                            return rs.next() ? Boolean.TRUE : Boolean.FALSE;
                        }
                    });
        } catch (final DatabaseStoreException e) {
        }
        return exists;
    }

    /**
     * @param directory
     * @throws DatabaseStoreException
     */
    void createIndexTable(final DatabaseDirectory directory) throws DatabaseStoreException {
        final Connection connection = DataSourceUtils.getConnection(directory.getDataSource());
        final String sqlCreate = directory.getDialect().sqlTableCreate(directory.getIndexTableName());
        JdbcTemplate.executeUpdate(connection, sqlCreate, true, new JdbcTemplate.PrepateStatementAwareCallback() {

            @Override
            public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                // do nothing
            }
        });
    }

    /**
     * @param directory
     * @return
     * @throws DatabaseStoreException
     */
    public String[] listAllFiles(final DatabaseDirectory directory) throws DatabaseStoreException {
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

    /**
     * @param directory
     * @param names
     */
    void syncFiles(final DatabaseDirectory directory, final Collection<String> names) {
        // TODO Auto-generated method stub

    }

    /**
     * @param directory
     * @param name
     * @return
     * @throws DatabaseStoreException
     */
    public long fileLength(final DatabaseDirectory directory, final String name) throws DatabaseStoreException {
        final Connection connection = DataSourceUtils.getConnection(directory.getDataSource());
        final String sqlSelectSize = directory.getDialect().sqlSelectSize(directory.getIndexTableName());
        return (long) JdbcTemplate.executeSelect(connection, sqlSelectSize, new JdbcTemplate.ExecuteSelectCallback() {

            @Override
            public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                ps.setString(1, name);
            }

            @Override
            public Object execute(final ResultSet rs) throws Exception {
                return rs.next() ? rs.getLong(1) : 0l;
            }
        });
    }

    /**
     * @param directory
     * @param source
     * @param dest
     * @throws DatabaseStoreException
     */
    public void renameFile(final DatabaseDirectory directory, final String source, final String dest)
            throws DatabaseStoreException {
        final Connection connection = DataSourceUtils.getConnection(directory.getDataSource());
        final String sqlUpdate = directory.getDialect().sqlUpdate(directory.getIndexTableName());
        JdbcTemplate.executeUpdate(connection, sqlUpdate, false, new JdbcTemplate.PrepateStatementAwareCallback() {

            @Override
            public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                ps.setFetchSize(1);
                ps.setString(1, dest);
                ps.setString(2, source);
            }
        });

    }

    /**
     * @param directory
     * @param name
     * @return
     * @throws DatabaseStoreException
     */
    public byte[] fileContent(final DatabaseDirectory directory, final String name) throws DatabaseStoreException {
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

    /**
     * @param directory
     * @param name
     * @param content
     * @param contentLength
     * @param commit
     * @throws DatabaseStoreException
     */
    public void saveFile(final DatabaseDirectory directory, final String name, final byte[] content,
            final int contentLength, final boolean commit) throws DatabaseStoreException {
        final String sqlInsert = directory.getDialect().sqlInsert(directory.getIndexTableName());
        final Connection connection = DataSourceUtils.getConnection(directory.getDataSource());
        JdbcTemplate.executeUpdate(connection, sqlInsert, commit, new JdbcTemplate.PrepateStatementAwareCallback() {

            @Override
            public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                ps.setFetchSize(1);
                ps.setString(1, name);
                if (content == null || content.length == 0) {
                    ps.setNull(2, Types.BLOB);
                } else {
                    ps.setBinaryStream(2, new ByteArrayInputStream(content), contentLength);
                }
                ps.setInt(3, contentLength);
            }
        });
    }

    /**
     * @param directory
     * @param name
     * @param commit
     * @throws DatabaseStoreException
     */
    public void deleteFile(final DatabaseDirectory directory, final String name, final boolean commit)
            throws DatabaseStoreException {
        final Connection connection = DataSourceUtils.getConnection(directory.getDataSource());
        final String sqlDelete = directory.getDialect().sqlDeleteByName(directory.getIndexTableName());
        JdbcTemplate.executeUpdate(connection, sqlDelete, commit, new JdbcTemplate.PrepateStatementAwareCallback() {

            @Override
            public void fillPrepareStatement(final PreparedStatement ps) throws Exception {
                ps.setFetchSize(1);
                ps.setString(1, name);
            }
        });
    }

    /**
     * @param directory
     * @param name
     * @return
     * @throws DatabaseStoreException
     */
    public boolean existsFile(final DatabaseDirectory directory, final String name) throws DatabaseStoreException {
        final Connection connection = DataSourceUtils.getConnection(directory.getDataSource());
        final String sqlSelectName = directory.getDialect().sqlSelectName(directory.getIndexTableName());
        return (boolean) JdbcTemplate.executeSelect(connection, sqlSelectName,
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
    }
}
