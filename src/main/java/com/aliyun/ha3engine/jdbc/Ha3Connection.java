package com.aliyun.ha3engine.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.aliyun.ha3engine.jdbc.common.config.Ha3Config;
import com.aliyun.ha3engine.jdbc.common.exception.ErrorCode;
import com.aliyun.ha3engine.jdbc.common.exception.Ha3DriverException;
import com.aliyun.ha3engine.jdbc.sdk.client.CloudClient;
import com.aliyun.ha3engine.jdbc.sdk.client.CloudClientHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ha3 JDBC connection
 *
 * @author yongxing.dyx
 * @date 2024/12/17
 */
public class Ha3Connection implements Connection, Ha3Wrapper {
    private static final Lock singletonLock = new ReentrantLock();
    private static final Map<String, CloudClientHolder> cloudClientHolders = new ConcurrentHashMap<>();

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final Ha3Config ha3Config;
    private boolean active;
    private final String clientKey;
    private CloudClient cloudClient = null;
    private static final int DEFAULT_POOL_SIZE = 10;

    private String catalog;
    private String schema;
    protected List<Ha3Statement> statements = new Vector<>();

    /**
     * Builds the ha3 {@link CloudClient} using the provided parameters.
     *
     */
    public Ha3Connection(Ha3Config ha3Config) throws Exception {
        long t0 = System.currentTimeMillis();
        try {
            printVitalProps(ha3Config);
            this.ha3Config = ha3Config;

            StringBuilder key = new StringBuilder();
            key.append(ha3Config.toString());
            this.clientKey = key.toString();

            CloudClient client;
            try {
                singletonLock.lock();
                CloudClientHolder cloudClientHolder = cloudClientHolders.get(getClientKey());
                if (cloudClientHolder != null) {
                    client = cloudClientHolder.getClient();
                } else {
                    long maxSizeConf = ha3Config.getMaxPoolSize();
                    maxSizeConf = maxSizeConf == 0 ? DEFAULT_POOL_SIZE : maxSizeConf;
                    if (cloudClientHolders.size() > maxSizeConf) {
                        throw new Ha3DriverException(ErrorCode.CONNECTION_SIZE_EXCEEDED_LIMIT,
                                String
                                        .format("The connection pool exceeded the limit: %d , if necessary,please increase "
                                                + "Ha3Config:maxPoolSize.", maxSizeConf));
                    }
                    client = new CloudClient(ha3Config);
                    cloudClientHolder = new CloudClientHolder(client);
                    cloudClientHolders.put(getClientKey(), cloudClientHolder);
                }
                cloudClientHolder.refInc();

            } finally {
                singletonLock.unlock();
            }

            this.cloudClient = client;
            this.active = true;
        } finally {
            long t1 = System.currentTimeMillis();
            long cost = t1 - t0;
            if (ha3Config.isEnableDetailLog()) {
                logger.warn("a new Ha3Connection has been create.cost:{}", cost);
            }
        }

    }

    @Override
    public Statement createStatement() throws SQLException {
        checkClosed();
        if (this.cloudClient == null) {
            throw new SQLException("Unable to connect on specified schema '" + this.schema + "'");
        }
        Ha3Statement st = new Ha3Statement(this, cloudClient);
        statements.add(st);
        return st;
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new Ha3PreparedStatement(this, cloudClient, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ha3Connection#prepareCall is not supported");
    }

    @Override
    public String nativeSQL(String sql) {
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        throw new SQLFeatureNotSupportedException("Non auto-commit is not supported");
    }

    @Override
    public boolean getAutoCommit() {
        return true;
    }

    @Override
    public void commit() throws SQLException {
        if (getAutoCommit()) { throw new SQLException("Auto-commit is enabled"); }
        throw new SQLFeatureNotSupportedException("Commit/Rollback not supported");
    }

    @Override
    public void rollback() throws SQLException {
        if (getAutoCommit()) { throw new SQLException("Auto-commit is enabled"); }
        throw new SQLFeatureNotSupportedException("Commit/Rollback not supported");
    }

    @Override
    public void close() throws SQLException {
        if (isClosed()) {
            return;
        }
        synchronized (this) {
            statements.clear();
            //如果是共用单例client
            try {
                singletonLock.lock();
                CloudClientHolder cloudClientHolder = cloudClientHolders.get(getClientKey());
                cloudClientHolder.refDec();
                //如果client的持有者都close,对于同一个客户端保留最后的一个，防止重复创建耗费资源
                if (ha3Config.isEnableDetailLog()) {
                    logger.warn("a connection close, key:{}, ref:{}", getClientKey(), cloudClientHolder.refCount());
                }
            } catch (Exception e) {
                logger.error("close ha3 cloud client connection error.", e);
            } finally {
                singletonLock.unlock();
            }
            this.active = false;
        }
    }

    @Override
    public boolean isClosed() {
        return !active;
    }

    @Override
    public DatabaseMetaData getMetaData() {
        return new Ha3DatabaseMetaData(this);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        if (!readOnly) { throw new SQLFeatureNotSupportedException("Only read-only mode is supported"); }
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    @Override
    public String getCatalog() {
        return this.catalog;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ha3Connection#setTransactionIsolation is not supported");
    }

    @Override
    public int getTransactionIsolation() {
        return Ha3Connection.TRANSACTION_NONE;
    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        return;
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
        throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ha3Connection#prepareCall is not supported");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException("Ha3Connection#typeMap not supported");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ha3Connection#setTypeMap is not supported");
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ha3Connection#setHoldability is not supported");
    }

    @Override
    public int getHoldability() {
        return -1;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("Ha3Connection#setSavepoint is not supported");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ha3Connection#setSavepoint is not supported");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ha3Connection#rollback is not supported");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ha3Connection#releaseSavepoint is not supported");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
        throws SQLException {
        checkClosed();
        if (this.cloudClient == null) {
            throw new SQLException("Unable to connect on specified schema '" + this.schema + "'");
        }
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
        int resultSetHoldability) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
        int resultSetHoldability) throws SQLException {
        return prepareCall(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ha3Connection#Autogenerated key not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ha3Connection#Autogenerated key not supported");
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("Ha3Connection#createClob is not supported");
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException("Ha3Connection#createBlob is not supported");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("Ha3Connection#createNClob is not supported");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("Ha3Connection#createSQLXML is not supported");
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0) { throw new SQLException("Negative timeout"); }
        return active;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new SQLClientInfoException();
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ha3Connection#getClientInfo is not supported");
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        throw new SQLFeatureNotSupportedException("Ha3Connection#getClientInfo is not supported");
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) {
        return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ha3Connection#createStruct is not supported");
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        this.schema = schema;

    }

    @Override
    public String getSchema() {
        return this.schema;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ha3Connection#abort is not supported");
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ha3Connection#setNetworkTimeout is not supported");
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException("Ha3Connection#getNetworkTimeout is not supported");
    }

    private void printVitalProps(Ha3Config ha3Config) {
        if (ha3Config.isEnableDetailLog()) {
            logger.warn("-------------------------Ha3 Connection Properties------------------------------");
            logger.warn(
                "now a new connection maybe acquire... serviceName = {} ",
                ha3Config.getServiceName());
            logger.warn("urlInfo:{}", ha3Config.toString());
        }
    }

    public Ha3Config getHa3Config() {
        return ha3Config;
    }

    public String getClientKey() {
        return clientKey;
    }

    public void checkClosed() throws SQLException {
        if (this.isClosed()) {
            throwConnectionClosedException();
        }
    }

    public void throwConnectionClosedException() throws SQLException {
        throw new SQLException("No operations allowed after connection closed");
    }
}
