package com.aliyun.ha3engine.jdbc.common.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件工具类
 *
 * @author chengshan.lxf
 * @date 2022/6/21
 */
public class FileTools {
    private static final Logger logger = LoggerFactory.getLogger(FileTools.class);

    /**
     * 通过指定的文件路径读取数据
     *
     * @param resourcePath
     * @return
     */
    public static String loadResource(String resourcePath) {

        BufferedReader reader = null;
        StringBuffer buffer = new StringBuffer();

        try {
            InputStream is = FileTools.class.getClassLoader().getResourceAsStream(resourcePath);
            reader = new BufferedReader(new InputStreamReader(is));
            String line = reader.readLine();
            while (line != null) {
                buffer.append(line);
                buffer.append("\n");
                line = reader.readLine();
            }
        } catch (Exception e) {
            logger.error("Failed to load resource path:" + resourcePath, e);
            return null;
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    logger.error("Failed to close reader", e);
                }
            }
        }
        return buffer.toString();

    }

}
