package com.aliyun.ha3engine.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.aliyun.ha3engine.jdbc.common.exception.ErrorInfo;
import com.aliyun.ha3engine.jdbc.common.utils.JsonUtils;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * Ha3 JDBC result set
 *
 * 数据结果
 * {
 * "columns": [{"name": "xx", "type": "xxx"},...],
 * "rows": [["xxx", xxx],[...],...]
 * }
 *
 * @author yongxing.dyx
 * @date 2024/12/17
 */
public class Ha3ResultSet implements ResultSet, Ha3Wrapper {

    private final Ha3Statement ha3Statement;
    private final List<String> keyList = new ArrayList<>();
    private final List<String> typeList = new ArrayList<>();
    //数据读取行是从1开始，另外列也是从1开始
    private int rowNumber = 0;
    private boolean wasNull = false;
    private boolean isAfterLast = false;

    // 错误信息
    private ErrorInfo errorInfo;

    public ErrorInfo getErrorInfo() {
        return errorInfo;
    }

    private List<JsonArray> jsonArrayList;

    //jsonArrayList提供一个get方法
    public List<JsonArray> getJsonArrayList() {
        return jsonArrayList;
    }

    public Ha3ResultSet(String result, Ha3Statement ha3Statement) {
        this.ha3Statement = ha3Statement;
        initJsonArrayList(result);
    }

    private void initJsonArrayList(String result) {
        if (jsonArrayList == null) {
            try {
                jsonArrayList = new ArrayList<>();

                JsonObject jsonObject = JsonUtils.fromJson(result, JsonObject.class);

                JsonArray colums = jsonObject.getAsJsonArray("columns");
                if (null != colums) {
                    for (int i = 0; i < colums.size(); i++) {
                        JsonObject col = colums.get(i).getAsJsonObject();
                        keyList.add(col.get("name").getAsString());
                        typeList.add(col.get("type").getAsString());
                    }
                }

                JsonArray rows = jsonObject.getAsJsonArray("rows");
                if (null != rows) {
                    for (int i = 0, size = rows.size(); i < size; i++) {
                        JsonArray row = rows.get(i).getAsJsonArray();

                        jsonArrayList.add(row);
                    }
                }

                //填充错误信息
                JsonObject error = jsonObject.getAsJsonObject("error");
                if (null != error) {
                    errorInfo = new ErrorInfo(error.get("errorCode").getAsLong(), error.get("message").getAsString(),
                        error.get("error").getAsString());
                }

            } catch (Exception e) {
                wasNull = true;
                errorInfo = new ErrorInfo(500,
                    "initJsonArrayList exception", "detail error" + e.getMessage());
            }
        } else {
            errorInfo = new ErrorInfo(404, "ha3 result is null",
                "jdbc inner initJsonArrayList jsonArrayList is null");
        }
    }

    private String getKey(int columnIndex) {
        return this.keyList.get(columnIndex - 1);
    }

    @Override
    public boolean next() {
        if (jsonArrayList != null && jsonArrayList.size() > rowNumber) {
            this.rowNumber++;
            return true;
        }

        if (jsonArrayList != null && jsonArrayList.size() != 0) {
            isAfterLast = true;
        }

        return false;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean wasNull() {
        return this.wasNull;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return getObject(columnIndex, String.class);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return getBoolean(getKey(columnIndex));
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return getByte(getKey(columnIndex));
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return getShort(getKey(columnIndex));
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return getInt(getKey(columnIndex));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return getLong(getKey(columnIndex));
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return getFloat(getKey(columnIndex));
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return getDouble(getKey(columnIndex));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("BigDecimal not supported");
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return getBytes(getKey(columnIndex));
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return getDate(getKey(columnIndex));
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return getTime(getKey(columnIndex));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return getTimestamp(getKey(columnIndex));
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("AsciiStream not supported");
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("UnicodeStream not supported");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("BinaryStream not supported");
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getObject(columnLabel, String.class);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        Boolean value = getObject(columnLabel, Boolean.class);
        return value == null ? false : value;
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        Byte value = getObject(columnLabel, Byte.class);
        return value == null ? 0 : value;
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        Short value = getObject(columnLabel, Short.class);
        return value == null ? 0 : value;
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        Integer value = getObject(columnLabel, Integer.class);
        return value == null ? 0 : value;
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        Long value = getObject(columnLabel, Long.class);
        return value == null ? 0 : value;
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        Float value = getObject(columnLabel, Float.class);
        return value == null ? 0F : value;
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        Double value = getObject(columnLabel, Double.class);
        return value == null ? 0D : value;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("BigDecimal not supported");
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getObject(columnLabel, byte[].class);
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        try {
            return getObject(columnLabel, Date.class);
        } catch (Exception e) {
            throw new SQLException(String.format(Locale.ROOT, "Unable to convert value [%.128s] of type [%s] to a Date",
                new Object[] {getObject(columnLabel, String.class),
                    typeList.get(keyList.indexOf(columnLabel))}), e);
        }
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        try {
            return getObject(columnLabel, Time.class);
        } catch (Exception e) {
            throw new SQLException(String.format(Locale.ROOT, "Unable to convert value [%.128s] of type [%s] to a Date",
                new Object[] {getObject(columnLabel, String.class),
                    typeList.get(keyList.indexOf(columnLabel))}), e);
        }
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        try {
            return getObject(columnLabel, Timestamp.class);
        } catch (Exception e) {
            throw new SQLException(String.format(Locale.ROOT, "Unable to convert value [%.128s] of type [%s] to a Date",
                new Object[] {getObject(columnLabel, String.class),
                    typeList.get(keyList.indexOf(columnLabel))}), e);
        }
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("AsciiStream not supported");
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("UnicodeStream not supported");
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("BinaryStream not supported");
    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {
    }

    @Override
    public String getCursorName() {
        return null;
    }

    @Override
    public ResultSetMetaData getMetaData() {
        return new Ha3ResultSetMetaData(this);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getObject(getKey(columnIndex));
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(columnLabel, Object.class);
    }

    @Override
    public int findColumn(String columnLabel) {
        return keyList.indexOf(columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("CharacterStream not supported");
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("CharacterStream not supported");
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return getBigDecimal(getKey(columnIndex));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getObject(columnLabel, BigDecimal.class);
    }

    @Override
    public boolean isBeforeFirst() {
        return jsonArrayList != null && !jsonArrayList.isEmpty() && this.rowNumber < 1;
    }

    @Override
    public boolean isAfterLast() {
        return isAfterLast;
    }

    @Override
    public boolean isFirst() {
        return (this.rowNumber == 1);
    }

    @Override
    public boolean isLast() {
        return jsonArrayList != null && !jsonArrayList.isEmpty()
            && !isAfterLast && this.rowNumber == jsonArrayList.size();
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw new SQLException("ResultSet is forward-only");
    }

    @Override
    public void afterLast() throws SQLException {
        throw new SQLException("ResultSet is forward-only");
    }

    @Override
    public boolean first() throws SQLException {
        throw new SQLException("ResultSet is forward-only");
    }

    @Override
    public boolean last() throws SQLException {
        throw new SQLException("ResultSet is forward-only");
    }

    @Override
    public int getRow() {
        return this.rowNumber;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        throw new SQLException("ResultSet is forward-only");
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw new SQLException("ResultSet is forward-only");
    }

    @Override
    public boolean previous() throws SQLException {
        throw new SQLException("ResultSet is forward-only");
    }

    @Override
    public int getFetchDirection() {
        return 1000;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != 1000) {
            throw new SQLException("Fetch direction must be FETCH_FORWARD");
        }
    }

    @Override
    public int getFetchSize() {
        return jsonArrayList.size();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException("Fetch size cannot be changed");
    }

    @Override
    public int getType() {
        return ResultSet.TYPE_SCROLL_INSENSITIVE;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public Statement getStatement() {
        return this.ha3Statement;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        if (map == null || map.isEmpty()) {
            return getObject(columnIndex);
        }
        throw new SQLFeatureNotSupportedException("getObject with non-empty Map not supported");
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ref not supported");
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob not supported");
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob not supported");
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array not supported");
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        if (map == null || map.isEmpty()) {
            return getObject(columnLabel);
        }
        throw new SQLFeatureNotSupportedException("getObject with non-empty Map not supported");
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ref not supported");
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob not supported");
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob not supported");
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array not supported");
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return getDate(columnIndex);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(columnLabel);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return getTime(columnIndex);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(columnLabel);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return getTimestamp(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(columnLabel);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("URL not supported");
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("URL not supported");
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("RowId not supported");
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("RowId not supported");
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public int getHoldability() {
        return 1;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob not supported");
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob not supported");
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML not supported");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML not supported");
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML not supported");
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML not supported");
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("NString not supported");
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("NString not supported");
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCharacterStream not supported");
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCharacterStream not supported");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCharacterStream not supported");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCharacterStream not supported");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Writes not supported");
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return getObject(getKey(columnIndex), type);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        if (type == null) {
            throw new SQLException("type is null");
        }
        try {
            return JsonUtils.fromJson(jsonArrayList.get(rowNumber - 1).get(keyList.indexOf(columnLabel)), type);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public List<String> getKeyList() {
        return keyList;
    }

    public List<String> getTypeList() {
        return typeList;
    }

}
