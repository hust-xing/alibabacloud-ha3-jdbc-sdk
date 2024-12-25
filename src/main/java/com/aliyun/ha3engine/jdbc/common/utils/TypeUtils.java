package com.aliyun.ha3engine.jdbc.common.utils;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashSet;
import java.util.Set;

/**
 * @author yongxing.dyx
 * @date 2024/12/17
 */
public class TypeUtils {

    private TypeUtils() {
    }

    public final static String TINYINT = "byte";
    public final static String BINARY = "byte[]";
    public final static String LONG = "long";
    public final static String DOUBLE = "double";
    public final static String SMALLINT = "short";
    public final static String FLOAT = "float";
    public final static String INTEGER = "int";
    public final static String STRING = "java.lang.String";
    public final static String LONGS = "long[]";
    public final static String INTEGERS = "int[]";
    public final static String STRINGS = "java.lang.String[]";
    public final static String FLOATS = "float[]";
    public final static String DOUBLES = "double[]";

    private static final Set<String> SIGNED_TYPE = new HashSet<String>() {{
        add(TypeUtils.TINYINT);
        add(TypeUtils.BINARY);
        add(TypeUtils.LONG);
        add(TypeUtils.DOUBLE);
        add(TypeUtils.DOUBLES);
        add(TypeUtils.SMALLINT);
        add(TypeUtils.FLOAT);
        add(TypeUtils.INTEGER);
        add(TypeUtils.STRING);
        add(TypeUtils.LONGS);
        add(TypeUtils.INTEGERS);
        add(TypeUtils.STRINGS);
        add(TypeUtils.FLOATS);
    }};

    public static boolean isSigned(String type) {
        return SIGNED_TYPE.contains(type);
    }

    public static int getTypeIdForObject(Object c) {
        if (c instanceof Long) {
            return Types.BIGINT;
        }
        if (c instanceof Boolean) {
            return Types.BOOLEAN;
        }
        if (c instanceof Character) {
            return Types.CHAR;
        }
        if (c instanceof java.sql.Date) {
            return Types.DATE;
        }
        if (c instanceof java.util.Date) {
            return Types.TIMESTAMP;
        }
        if (c instanceof Double) {
            return Types.DOUBLE;
        }
        if (c instanceof Integer) {
            return Types.INTEGER;
        }
        if (c instanceof BigDecimal) {
            return Types.NUMERIC;
        }
        if (c instanceof Short) {
            return Types.SMALLINT;
        }
        if (c instanceof Float) {
            return Types.FLOAT;
        }
        if (c instanceof String) {
            return Types.VARCHAR;
        }
        if (c instanceof Time) {
            return Types.TIME;
        }
        if (c instanceof Timestamp) {
            return Types.TIMESTAMP;
        }
        if (c instanceof Byte) {
            return Types.TINYINT;
        }
        if (c instanceof Byte[]) {
            return Types.VARBINARY;
        }
        if (c instanceof Object[]) {
            return Types.JAVA_OBJECT;
        }
        if (c instanceof Object) {
            return Types.JAVA_OBJECT;
        }
        if (c instanceof Array) {
            return Types.ARRAY;
        } else {
            return Types.OTHER;
        }
    }
}
