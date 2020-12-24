package com.flyAI.flinkJob.LogAnalysis;

import com.alibaba.fastjson.JSON;
import com.flyAI.flinkJob.LogAnalysis.constant.DataConstant;
import com.flyAI.flinkJob.LogAnalysis.count.IpCount;
import com.flyAI.flinkJob.LogAnalysis.utils.ClickHouseUtils;
import com.flyAI.flinkJob.LogAnalysis.utils.ConfigUtils;
import com.flyAI.flinkJob.LogAnalysis.vo.NginxVo;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.calcite.shaded.com.google.common.base.Throwables;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author lizhe
 */
@Slf4j
public class LogFlinkSaveJobApplication {


    public static void main(String[] args) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.enableCheckpointing(DataConstant.CHECK_POINTING_TIME);
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
            env.getCheckpointConfig().setCheckpointTimeout(60000);
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
            env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
            new ConfigUtils();
            Properties properties = ConfigUtils.getActivityConsumerKafkaSaveConfig();
            String consumer_topic = ConfigUtils.getKafkaConsumerTopicConfig();
            FlinkKafkaConsumer <String> consumer = new FlinkKafkaConsumer <>(consumer_topic, new SimpleStringSchema(), properties);
            consumer.setStartFromGroupOffsets();
            env.addSource(consumer).name("log-job-save")
                    .filter(new FilterFunction <String>() {
                        @Override
                        public boolean filter(String s) throws Exception {
                            return s != null && !s.isEmpty();
                        }
                    })
                    .map(new MapFunction <String, NginxVo>() {
                        @Override
                        public NginxVo map(String s) throws Exception {
                            NginxVo nginxVo = JSON.parseObject(s, NginxVo.class);
                            return nginxVo;
                        }
                    }).addSink(new SinkFunction <NginxVo>() {
                @Override
                public void invoke(NginxVo value, SinkFunction.Context context) throws Exception {
                    System.err.println("===input");
                    List <Object> nginxList = new ArrayList <>();
                    new ClickHouseUtils();
                    nginxList.add(value);
                    ClickHouseUtils.createAndInsert("log_nginx_access_app", nginxList);
                }
            }).name("save original data");
            env.execute("log save flink job");
        } catch (Exception e) {
            log.error("flink job run exception : {}", Throwables.getStackTraceAsString(e));
        }
    }
}
