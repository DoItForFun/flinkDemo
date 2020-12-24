package com.flyAI.flinkJob.LogAnalysis.utils;


import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.*;

/**
 * @author lizhe
 */
@Slf4j
public class ClickHouseUtils {

    private static String database = null;
    private static String account = null;
    private static String password = null;
    private static String url = null;
    private static Map result = new HashMap <>();

    public ClickHouseUtils() {
        new ConfigUtils();
        database = ConfigUtils.getClickDb();
        account = ConfigUtils.getClickAccount();
        password = ConfigUtils.getClickPassword();
        url = ConfigUtils.getInsertUrl();
    }


    public static Map <String, Object> getCommonConfig() {
        Map <String, Object> commonConfig = new HashMap <>();
        commonConfig.put("account", account);
        commonConfig.put("password", password);
        commonConfig.put("database", database);
        commonConfig.put("connect", database);
        return commonConfig;
    }


    public static boolean createAndInsert(String table, List <?> data) throws IOException {
        Map <String, Object> param = new HashMap <>();
        param.put("table", table);
        param.put("data", data);
        param.put("table_create_type", "auto");
        send(url, param);
        return check();
    }

    private static boolean check() {
        return !result.isEmpty()
                && result.containsKey("code")
                && result.get("code").equals(200);
    }

    private static void send(String url, Map <String, Object> params) throws IOException {
        params.putAll(getCommonConfig());
        String response = HttpConnectUtils.doPost(url, JSON.toJSONString(params));
        result = JSON.parseObject(response, Map.class);
    }


}
