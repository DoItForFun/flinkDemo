package com.flyAI.flinkJob.LogAnalysis.utils;

import java.util.Properties;

/**
 * @author lizhe
 */
public class ConfigUtils {

    private static Properties configProperties;


    public ConfigUtils(){
        Properties properties = new Properties();
        try {
            properties.load(this.getClass().getResourceAsStream("/application.properties"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        configProperties = properties;
    }
    /**
     * 组装 activity kafka 队列的消费端基础信息
     *
     * @return
     */
    public static Properties getActivityConsumerKafkaBasicConfig() {
        Properties kafkaBaseConf = new Properties();
        kafkaBaseConf.setProperty("bootstrap.servers", configProperties.getProperty("consumer.bootstrap.servers"));
        kafkaBaseConf.setProperty("group.id", configProperties.getProperty("log.consumer.group.id"));
        return kafkaBaseConf;
    }

    /**
     * 组装 activity kafka 队列的消费端基础信息
     *
     * @return
     */
    public static Properties getActivityConsumerKafkaSaveConfig() {
        Properties kafkaBaseConf = new Properties();
        kafkaBaseConf.setProperty("bootstrap.servers", configProperties.getProperty("consumer.bootstrap.servers"));
        kafkaBaseConf.setProperty("group.id", configProperties.getProperty("log.consumer.group.save.id"));
        return kafkaBaseConf;
    }

    /**
     * 组装 kafka 的消费端基础信息
     *
     * @return
     */
    public static Properties getProducerKafkaBasicConfig() {
        Properties kafkaBaseConf = new Properties();
        kafkaBaseConf.setProperty("bootstrap.servers", configProperties.getProperty("producer.bootstrap.servers"));
        return kafkaBaseConf;
    }

    /**
     * 获取生产producer topic
     *
     * @return
     */
    public static String getKafkaProducerTopicConfig() {
        return configProperties.get("log.producer.topic").toString();
    }

    /**
     * 获取消费kafka topic
     * @return
     */
    public static String getKafkaConsumerTopicConfig() {
        return configProperties.get("log.consumer.topic").toString();
    }

    /**
     * 获取click house
     * @return
     */
    public static String getClickDb(){
        return configProperties.get("clickHouse.api.db").toString();
    }

    /**
     * 获取click insert url
     * @return
     */
    public static String getInsertUrl(){
        return configProperties.get("clickHouse.api.url").toString();
    }

    /**
     * 获取插入账号
     * @return
     */
    public static String getClickAccount(){
        return configProperties.get("clickHouse.config.account").toString();
    }

    /**
     * 获取插入密码
     * @return
     */
    public static String getClickPassword(){
        return configProperties.get("clickHouse.config.password").toString();
    }

}
