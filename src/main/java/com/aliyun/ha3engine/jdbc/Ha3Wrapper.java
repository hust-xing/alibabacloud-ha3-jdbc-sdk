package com.aliyun.ha3engine.jdbc;

import java.sql.SQLException;
import java.sql.Wrapper;

/**
 * @author yongxing.dyx
 * @date 2024/12/17
 */
public interface Ha3Wrapper extends Wrapper {

    default boolean isWrapperFor(Class<?> iface) throws SQLException {
        return (iface != null && iface.isAssignableFrom(getClass()));
    }

    default <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) { return (T)this; }
        throw new SQLException();
    }
}
