package com.flyAI.flinkJob.LogAnalysis;

import com.flyAI.flinkJob.LogAnalysis.utils.ClickHouseUtils;
import com.flyAI.flinkJob.LogAnalysis.utils.ConfigUtils;
import com.flyAI.flinkJob.LogAnalysis.constant.DataConstant;
import com.alibaba.fastjson.JSON;
import com.flyAI.flinkJob.LogAnalysis.count.IpCount;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.calcite.shaded.com.google.common.base.Throwables;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import com.flyAI.flinkJob.LogAnalysis.vo.NginxVo;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @author lizhe
 */
@Slf4j
public class LogFlinkJobApplication {

    private static SingleOutputStreamOperator <String> streamSource;
    private static SingleOutputStreamOperator<NginxVo> outputStream;
    private static StreamExecutionEnvironment env;
    private static final OutputTag <NginxVo> original = new OutputTag <NginxVo>("ORIGINAL"){};
    private static final OutputTag <NginxVo> middleware = new OutputTag <NginxVo>("MIDDLEWARE"){};

    public static void main(String[] args) {
        try {
            setEnv();
            setCheckPointInfo(env);
            setStreamSource(env);
            sideOutPut();
            reduce();
            env.execute("log flink job");
        } catch (Exception e) {
            log.error("flink job run exception : {}", Throwables.getStackTraceAsString(e));
        }
    }

    private static void setEnv() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);
    }

    private static void setCheckPointInfo(StreamExecutionEnvironment env) {
        env.enableCheckpointing(DataConstant.CHECK_POINTING_TIME);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    }

    private static void setStreamSource(StreamExecutionEnvironment executionEnvironment) {
        new ConfigUtils();
        Properties properties = ConfigUtils.getActivityConsumerKafkaBasicConfig();
        String consumer_topic = ConfigUtils.getKafkaConsumerTopicConfig();
        FlinkKafkaConsumer <String> consumer = new FlinkKafkaConsumer <>(consumer_topic, new SimpleStringSchema(), properties);
        consumer.setStartFromGroupOffsets();
        streamSource = executionEnvironment.addSource(consumer);
    }

    private static void sideOutPut() {
        outputStream = streamSource
                .name("log-job")
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
                });
    }



    private static void reduce() {
        outputStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor <NginxVo>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(NginxVo nginxVo) {
                return nginxVo.getTimestamp().getTime();
            }
        })
                .flatMap(new FlatMapFunction <NginxVo, IpCount>() {
                    @Override
                    public void flatMap(NginxVo nginxVo, Collector <IpCount> collector) throws Exception {
                        String xff = nginxVo.getXff();
                        if (xff == null) {
                            return;
                        }
                        List <?> ipList = Arrays.asList(xff.split(","));
                        if (ipList.isEmpty()) {
                            return;
                        }
                        String realIp = ipList.get(0).toString();
                        nginxVo.setRemoteIp(realIp);
                        collector.collect(new IpCount(realIp, 1, nginxVo.getTimestamp()));
                    }
                })
                .keyBy("remoteIp")
                .timeWindow(Time.minutes(1))
                .reduce(new ReduceFunction <IpCount>() {
                    @Override
                    public IpCount reduce(IpCount t0, IpCount t1) throws Exception {
                        long count = t0.count + t1.count;
                        IpCount ipCount = new IpCount(t0.remoteIp, count, t0.getStart());
                        ipCount.setEnd(t1.getStart());
                        return ipCount;
                    }
                }).addSink(new SinkFunction <IpCount>() {
            @Override
            public void invoke(IpCount value, Context context) throws Exception {
                System.err.println("==input");
                List <Object> list = new ArrayList <>();
                if(value.getEnd() == null){
                    value.setEnd(value.getStart());
                }
                list.add(value);
                new ClickHouseUtils();
                ClickHouseUtils.createAndInsert("log_ip_count_reduce", list);
            }
        }).name("save ip count data");
    }
}
