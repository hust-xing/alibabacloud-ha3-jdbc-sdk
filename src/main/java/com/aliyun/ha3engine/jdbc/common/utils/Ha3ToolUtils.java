package com.aliyun.ha3engine.jdbc.common.utils;

import java.nio.charset.Charset;

/**
 * Ha3ToolUtils，包含一些字符串处理/判断工具类
 *
 * @author yongxing.dyx
 * @date 2024/12/17
 */
public class Ha3ToolUtils {
    public static final String URL_PREFIX = "jdbc:ha3://";

    public static boolean canAccept(String url) {
        return !isEmpty(url) && url.trim().startsWith(URL_PREFIX);
    }

    public static boolean isEmpty(CharSequence str) {
        return str == null || str.length() == 0;
    }

    public static boolean isBlank(CharSequence str) {
        int length;

        if ((str == null) || ((length = str.length()) == 0)) {
            return true;
        }

        for (int i = 0; i < length; i++) {
            // 只要有一个非空字符即为非空字符串
            if (false == isBlankChar(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public static boolean isBlankChar(int c) {
        return Character.isWhitespace(c)
            || Character.isSpaceChar(c)
            || c == '\ufeff'
            || c == '\u202a'
            || c == '\u0000';
    }

    public static int parseInt(String strNum, int defVal) throws NumberFormatException {
        if (isBlank(strNum)) {
            return defVal;
        }
        if (strNum.toLowerCase().startsWith("0x")) {
            // 0x04表示16进制数
            return Integer.parseInt(strNum.substring(2), 16);
        }
        try {
            return Integer.parseInt(strNum);
        } catch (NumberFormatException e) {
            return defVal;
        }
    }

    public static byte[] bytes(CharSequence str, Charset charset) {
        if (str == null) {
            return null;
        }

        if (null == charset) {
            return str.toString().getBytes();
        }
        return str.toString().getBytes(charset);
    }

    /**
     * 判断'searchIn'是否包含'searchFor'，忽略大小写和前导空格
     *
     * @param searchIn
     * @param searchFor
     * @param beginPos
     * @return true/false
     */

    public static boolean startsWithIgnoreCaseAndWs(String searchIn, String searchFor, int beginPos) {
        if (searchIn == null) {
            return searchFor == null;
        }

        int inLength = searchIn.length();

        for (; beginPos < inLength; beginPos++) {
            if (!Character.isWhitespace(searchIn.charAt(beginPos))) {
                break;
            }
        }

        return startsWithIgnoreCase(searchIn, beginPos, searchFor);
    }

    /**
     * 'searchIn'是否包含'searchFor'
     *
     * @param searchIn
     * @param startAt
     * @param searchFor
     * @return true/false
     */
    public static boolean startsWithIgnoreCase(String searchIn, int startAt, String searchFor) {
        return searchIn.regionMatches(true, startAt, searchFor, 0, searchFor.length());
    }

    /**
     * 将sql的formatType调整为full_json
     *
     * @param sql
     * @return
     */
    public static String getFullJsonSql(String sql) {
        /** only support formatType:full_json */
        if (sql.contains("&&kvpair=")) {
            if (!sql.contains("formatType")) {
                sql = sql.replace("&&kvpair=", "&&kvpair=formatType:full_json;");
            } else {
                if (sql.contains("formatType:string")) {
                    sql = sql.replace("formatType:string", "formatType:full_json");
                } else if (sql.contains("formatType:json")) {
                    sql = sql.replace("formatType:json", "formatType:full_json");
                } else if (sql.contains("formatType:flatbuffers")) {
                    sql = sql.replace("formatType:flatbuffers", "formatType:full_json");
                }
            }
        } else {
            StringBuilder stringBuilder = new StringBuilder(sql);
            stringBuilder.append("&&kvpair=formatType:full_json;");
            sql = stringBuilder.toString();
        }
        return sql;
    }

    /**
     * 将sql的formatType调整为flatbuffers
     *
     * @param sql
     * @return
     */
    public static String getFlatBuffersSql(String sql) {
        /** only support formatType:flatbuffers */
        if (sql.contains("&&kvpair=")) {
            if (!sql.contains("formatType")) {
                sql = sql.replace("&&kvpair=", "&&kvpair=formatType:flatbuffers;");
            } else {
                if (sql.contains("formatType:string")) {
                    sql = sql.replace("formatType:string", "formatType:flatbuffers");
                } else if (sql.contains("formatType:json")) {
                    sql = sql.replace("formatType:json", "formatType:flatbuffers");
                } else if (sql.contains("formatType:full_json")) {
                    sql = sql.replace("formatType:full_json", "formatType:flatbuffers");
                }
            }
        } else {
            StringBuilder stringBuilder = new StringBuilder(sql);
            stringBuilder.append("&&kvpair=formatType:flatbuffers;");
            sql = stringBuilder.toString();
        }
        return sql;
    }
}
