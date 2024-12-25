package com.aliyun.ha3engine.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLFeatureNotSupportedException;

import com.aliyun.ha3engine.jdbc.common.utils.TypeUtils;

/**
 * Ha3 JDBC result set meta data
 *
 * @author yongxing.dyx
 * @date 2024/12/17
 */
public class Ha3ResultSetMetaData implements ResultSetMetaData, Ha3Wrapper {

    private final Ha3ResultSet ha3ResultSet;

    public Ha3ResultSetMetaData(Ha3ResultSet ha3ResultSet) {
        this.ha3ResultSet = ha3ResultSet;
    }

    @Override
    public int getColumnCount() {
        return ha3ResultSet.getKeyList().size();
    }

    @Override
    public boolean isAutoIncrement(int column) {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) {
        return false;
    }

    @Override
    public boolean isSearchable(int column) {
        return false;
    }

    @Override
    public boolean isCurrency(int column) {
        return false;
    }

    @Override
    public int isNullable(int column) {
        return 0;
    }

    @Override
    public boolean isSigned(int column) {
        return TypeUtils.isSigned(ha3ResultSet.getTypeList().get(column - 1));
    }

    @Override
    public int getColumnDisplaySize(int column) {
        return ha3ResultSet.getKeyList().size();
    }

    @Override
    public String getColumnLabel(int column) {
        return ha3ResultSet.getKeyList().get(column - 1);
    }

    @Override
    public String getColumnName(int column) {
        return ha3ResultSet.getKeyList().get(column - 1);
    }

    @Override
    public String getSchemaName(int column) {
        return "";
    }

    @Override
    public int getPrecision(int column) {
        return 0;
    }

    @Override
    public int getScale(int column) {
        return 0;
    }

    @Override
    public String getTableName(int column) {
        return "";
    }

    @Override
    public String getCatalogName(int column) {
        return "";
    }

    @Override
    public int getColumnType(int column) throws SQLFeatureNotSupportedException {
        String type = ha3ResultSet.getTypeList().get(column - 1);
        switch (type) {
            case TypeUtils.TINYINT:
                return -6;
            case TypeUtils.BINARY:
                return -3;
            case TypeUtils.DOUBLE:
                return 8;
            case TypeUtils.DOUBLES:
                return 12;
            case TypeUtils.FLOAT:
                return 6;
            case TypeUtils.FLOATS:
                return 12;
            case TypeUtils.INTEGER:
                return 4;
            case TypeUtils.INTEGERS:
                return 12;
            case TypeUtils.LONG:
                return -5;
            case TypeUtils.LONGS:
                return 12;
            case TypeUtils.SMALLINT:
                return 5;
            case TypeUtils.STRING:
                return 12;
            case TypeUtils.STRINGS:
                return 12;
            default:
                return 0;
        }
    }

    @Override
    public String getColumnTypeName(int column) {
        return ha3ResultSet.getTypeList().get(column - 1);
    }

    @Override
    public boolean isReadOnly(int column) {
        return true;
    }

    @Override
    public boolean isWritable(int column) {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLFeatureNotSupportedException {
        String type = ha3ResultSet.getTypeList().get(column - 1);
        switch (type) {
            case TypeUtils.TINYINT:
                return "Byte.class";
            case TypeUtils.BINARY:
                return "byte[].class";
            case TypeUtils.DOUBLE:
                return "Double.class";
            case TypeUtils.DOUBLES:
                return "Double[].class";
            case TypeUtils.FLOAT:
                return "Float.class";
            case TypeUtils.FLOATS:
                return "Float[].class";
            case TypeUtils.INTEGER:
                return "Integer.class";
            case TypeUtils.INTEGERS:
                return "Integer[].class";
            case TypeUtils.LONG:
                return "Long.class";
            case TypeUtils.LONGS:
                return "Long[].class";
            case TypeUtils.SMALLINT:
                return "Short.class";
            case TypeUtils.STRING:
                return "String.class";
            case TypeUtils.STRINGS:
                return "String[].class";
            default:
                return "String.class";
        }
    }
}
