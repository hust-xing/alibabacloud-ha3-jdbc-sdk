package com.aliyun.ha3engine.jdbc.common.utils;

import java.lang.reflect.Type;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.JsonSyntaxException;
import org.apache.commons.lang3.StringUtils;

/**
 * gson序列化和反序列化工具
 *
 * @author yongxing.dyx
 * @date 2024/12/17
 */
public class JsonUtils {

    public static <T> T fromJson(String jsonString, Class<T> type) {
        Gson gson = new Gson();
        return gson.fromJson(jsonString, type);
    }

    public static <T> T fromJson(JsonElement jsonElement, Class<T> type) {
        Gson gson = new GsonBuilder()
            .registerTypeAdapter(Long.class, new LongDefault0Adapter())
            .registerTypeAdapter(long.class, new LongDefault0Adapter())
            .registerTypeAdapter(Integer.class, new InterDefault0Adapter())
            .registerTypeAdapter(int.class, new InterDefault0Adapter())
            .create();
        return gson.fromJson(jsonElement, type);
    }

    public static <T> String toJson(T object) {
        return toJson(object, false);
    }

    public static <T> String toJson(T object, boolean serializeNulls) {
        Gson gson;
        if (serializeNulls) {
            gson = (new GsonBuilder()).serializeNulls().create();
        } else {
            gson = new Gson();
        }

        return gson.toJson(object);
    }

    static class InterDefault0Adapter implements JsonSerializer<Integer>, JsonDeserializer<Integer> {

        @Override
        public JsonElement serialize(Integer src, Type typeOfSrc, JsonSerializationContext context) {
            return new JsonPrimitive(src);
        }

        @Override
        public Integer deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
            throws JsonParseException {
            try {
                //定义为int类型,如果后台返回""或者null,则返回0
                if (json.getAsString().equals("") || json.getAsString().equals("null")) {
                    return 0;
                }
            } catch (Exception ignore) {
            }
            try {
                return json.getAsInt();
            } catch (NumberFormatException e) {
                throw new JsonSyntaxException(e);
            }
        }
    }

    static class LongDefault0Adapter implements JsonDeserializer<Long>, JsonSerializer<Long> {

        @Override
        public Long deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
            throws JsonParseException {
            try {
                //定义为long类型,如果后台返回""或者null,则返回0
                if (json.getAsString().equals("") || json.getAsString().equals("null")) {
                    return 0l;
                }
            } catch (Exception ignore) {
            }
            try {
                return json.getAsLong();
            } catch (NumberFormatException e) {
                throw new JsonSyntaxException(e);
            }
        }

        @Override
        public JsonElement serialize(Long src, Type typeOfSrc, JsonSerializationContext context) {
            return new JsonPrimitive(src);
        }
    }

    /**
     * 将字符串转化成unicode码
     *
     * @param string
     * @return
     */
    public static String string2Unicode(String string) {

        if (StringUtils.isBlank(string)) {
            return null;
        }

        char[] bytes = string.toCharArray();
        StringBuffer unicode = new StringBuffer();
        for (int i = 0; i < bytes.length; i++) {
            char c = bytes[i];

            // 标准ASCII范围内的字符，直接输出
            if (c >= 0 && c <= 127) {
                unicode.append(c);
                continue;
            }
            String hexString = Integer.toHexString(bytes[i]);

            unicode.append("\\u");

            // 不够四位进行补0操作
            if (hexString.length() < 4) {
                unicode.append("0000".substring(hexString.length(), 4));
            }
            unicode.append(hexString);
        }
        return unicode.toString();
    }
}
