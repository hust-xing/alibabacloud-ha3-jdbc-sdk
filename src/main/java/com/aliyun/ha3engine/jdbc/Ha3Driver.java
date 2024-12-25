package com.aliyun.ha3engine.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import com.aliyun.ha3engine.jdbc.common.config.Ha3Config;
import com.aliyun.ha3engine.jdbc.common.utils.Ha3ToolUtils;
import lombok.SneakyThrows;

/**
 * Ha3 JDBC driver
 *
 * @author yongxing.dyx
 * @date 2024/12/17
 */
public class Ha3Driver implements Driver {

    static {
        try {
            DriverManager.registerDriver(new Ha3Driver());
        } catch (SQLException e) {
            throw new RuntimeException("Can't register driver!");
        }
    }

    @SneakyThrows
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (url == null) {
            throw new SQLException("url is required");
        }

        if (!acceptsURL(url)) {
            return null;
        }

        Ha3Config ha3Config = new Ha3Config(url, info);
        return new Ha3Connection(ha3Config);

    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (Ha3ToolUtils.canAccept(url)) {
            return true;
        }
        throw new SQLException("Expected [jdbc:ha3://] url,received [" + url + "]");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return Integer.valueOf(Ha3DatabaseMetaData.DRIVER_VERSION.split("\\.")[0]);
    }

    @Override
    public int getMinorVersion() {
        return Integer.valueOf(Ha3DatabaseMetaData.DRIVER_VERSION.split("\\.")[1]);
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Ha3Driver#getParentLogger is not supported");
    }
}
