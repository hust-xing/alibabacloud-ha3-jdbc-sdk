package com.aliyun.ha3engine.jdbc.common.config;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.aliyun.ha3engine.jdbc.common.utils.Ha3ToolUtils;

import org.apache.commons.lang3.StringUtils;

import lombok.Data;

/**
 * ha3 jdbc-client支持的完整参数，创建connection的时候，通过Properties设置
 *
 * @author yongxing.dyx
 * @date 2024/12/17
 */
@Data
public class Ha3Config {
    /**
     * jdbc用于校验的url，需要以jdbc:ha3:// 为前缀
     */
    private String jdbcUrl;
    /**
     * 用户名
     */
    private String username;
    /**
     * 密码
     */
    private String password;
    /**
     * endpoint的域名
     */
    private String serviceName;
    /**
     * 设置连接池属性，最大的连接数
     */
    private long maxPoolSize;

    /**
     * 是否开启详细日志打印
     */
    private boolean enableDetailLog = false;

    /**
     * 是否开启动态参数自动替换
     */
    private boolean enableDynamicParams = false;

    public boolean isLocalMode() {
        return localMode;
    }

    /**
     * for test，方便测试使用
     */
    private boolean localMode = false;

    public Ha3Config(String jdbcUrl, Properties props) throws SQLException {
        init(jdbcUrl, props);
    }

    private void init(String jdbcUrl, Properties props) throws SQLException {
        this.jdbcUrl = jdbcUrl;
        String[] jdbcParts = jdbcUrl.split("\\?");
        if (jdbcParts.length > 0) {
            String urlType = jdbcParts[0].replace(Ha3ToolUtils.URL_PREFIX, "");
            if (StringUtils.isEmpty(urlType)) {
                this.username = props.getProperty("user", props.getProperty("username"));
                this.password = props.getProperty("pass", props.getProperty("password"));

                this.serviceName = props.getProperty("serviceName");

                String enableDetailLog = props.getProperty("enableDetailLog");
                if ("true".equals(enableDetailLog)) {
                    this.enableDetailLog = true;
                }

                String enableDynamicParams = props.getProperty("enableDynamicParams");
                if ("true".equals(enableDynamicParams)) {
                    this.enableDynamicParams = true;
                }

                String mode = props.getProperty("mode");
                if ("local".equals(mode)) {
                    localMode = true;
                }
            } else {
                String[] urlParams = jdbcParts[1].split("&");
                Map<String, String> paramMap = new HashMap();
                for (int i = 0; i < urlParams.length; i++) {
                    String[] keyValue = urlParams[i].split("=");
                    paramMap.put(keyValue[0], keyValue[1]);
                }
                String enableDetailLog = paramMap.get("enableDetailLog");
                if ("true".equals(enableDetailLog)) {
                    this.enableDetailLog = true;
                }

                String enableDynamicParams = paramMap.get("enableDynamicParams");
                if ("true".equals(enableDynamicParams)) {
                    this.enableDynamicParams = true;
                }

                this.serviceName = paramMap.get("serviceName");
                this.username = paramMap.get("username");
                this.password = paramMap.get("password");
            }
        }
    }

    @Override
    public String toString() {
        return "Ha3Config{" +
            "jdbcUrl='" + jdbcUrl + '\'' +
            ", username='" + username + '\'' +
            ", password='" + password + '\'' +
            ", serviceName='" + serviceName + '\'' +
            '}';
    }
}
