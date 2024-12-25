package com.aliyun.ha3engine.jdbc.common.exception;

/**
 * @author yongxing.dyx
 * @date 2024/10/15
 */
public class ErrorInfo {

    /**
     * 错误码
     */
    private long errorCode;

    /**
     * 错误信息简介
     */
    private String message;

    /**
     * 具体异常信息
     */
    private String error;

    public ErrorInfo(long errorCode, String message, String error) {
        this.errorCode = errorCode;
        this.message = message;
        this.error = error;
    }

    public long getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(long errorCode) {
        this.errorCode = errorCode;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }
}
