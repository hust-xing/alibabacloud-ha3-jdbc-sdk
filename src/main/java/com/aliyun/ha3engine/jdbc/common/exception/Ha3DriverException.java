package com.aliyun.ha3engine.jdbc.common.exception;

import java.sql.SQLException;

/**
 * @author yongxing.dyx
 * @date 2024/12/17
 */
public class Ha3DriverException extends SQLException {
    private final ErrorCode errorCode;

    public Ha3DriverException(ErrorCode errorCode, String msg, Object... args) {
        super(format(errorCode, msg));
        this.errorCode = errorCode;
    }

    public Ha3DriverException(ErrorCode errorCode) {
        super(format(errorCode, ""));
        this.errorCode = errorCode;
    }

    public static String format(ErrorCode errorCode, String msg) {
        return errorCode.toString() + ",msg:" + msg;
    }

    public ErrorCode getError() {
        return errorCode;
    }
}
