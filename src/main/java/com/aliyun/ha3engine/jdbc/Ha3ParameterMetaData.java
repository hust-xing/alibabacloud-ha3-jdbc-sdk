package com.aliyun.ha3engine.jdbc;

import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import com.aliyun.ha3engine.jdbc.common.utils.TypeUtils;

/**
 * Ha3 parameter meta data
 *
 * @author yongxing.dyx
 * @date 2024/12/17
 */
public class Ha3ParameterMetaData implements ParameterMetaData, Ha3Wrapper {
    private Object[] sqlAndParams;

    public Ha3ParameterMetaData(Object[] sqlAndParams) {
        this.sqlAndParams = sqlAndParams;
    }

    @Override
    public int getParameterCount() {
        return sqlAndParams.length - 1;
    }

    @Override
    public int isNullable(int param) {
        return ParameterMetaData.parameterNullable;
    }

    @Override
    public boolean isSigned(int param) {
        return false;
    }

    @Override
    public int getPrecision(int param) {
        return 0;
    }

    @Override
    public int getScale(int param) {
        return 0;
    }

    @Override
    public int getParameterType(int param) {
        Object value = sqlAndParams[param];
        if (value == null) {
            return Types.OTHER;
        }
        return TypeUtils.getTypeIdForObject(value);
    }

    @Override
    public String getParameterTypeName(int param) {
        Object value = sqlAndParams[param];
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return "VARCHAR";
        } else if (value instanceof Integer) {
            return "INTEGER";
        } else if (value instanceof Long) {
            return "BIGINT";
        } else if (value instanceof Float) {
            return "FLOAT";
        } else if (value instanceof Double) {
            return "DOUBLE";
        } else if (value instanceof Boolean) {
            return "BOOLEAN";
        } else if (value instanceof Time) {
            return "TIME";
        } else if (value instanceof Timestamp) {
            return "TIMESTAMP";
        } else if (value instanceof Date) {
            return "DATE";
        } else {
            return "UNKNOWN";
        }
    }

    @Override
    public String getParameterClassName(int param) {
        Object value = sqlAndParams[param];
        if (value == null) {
            return null;
        }
        return value.getClass().getName();
    }

    @Override
    public int getParameterMode(int param) {
        return ParameterMetaData.parameterModeUnknown;
    }
}
