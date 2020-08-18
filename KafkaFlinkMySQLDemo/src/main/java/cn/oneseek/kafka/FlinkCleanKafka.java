package cn.oneseek.kafka;

import cn.oneseek.utils.DateUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;

import java.util.Properties;


public class FlinkCleanKafka {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");//kafka的节点的IP或者hostName，多个使用逗号分隔
        properties.setProperty("zookeeper.connect", "localhost:2181");//zookeeper的节点的IP或者hostName，多个使用逗号进行分隔
        properties.setProperty("group.id", "test-consumer-group");//flink consumer flink的消费者的group.id

        FlinkKafkaConsumer011<String> myConsumer = new FlinkKafkaConsumer011<String>(
                "test", new SimpleStringSchema(), properties);

        myConsumer.setStartFromLatest();
        /*
        每个数据流都以一个或多个源开头(Source)
        并以一个或多个接收器结束Sink
         */
        DataStream<String> stream = env.addSource(myConsumer);

        stream.print().setParallelism(2);


        DataStream CleanData = stream.map(new MapFunction<String, Tuple5<String, String, String, String, String>>() {
            @Override
            public Tuple5<String, String, String, String, String> map(String value) throws Exception {
                String[] data = value.split("\\s{4}");
                String CourseID = null;
                String url = data[2].split("\\ ")[1];
                if (url.startsWith("/class")) {
                    String CourseHTML = url.split("\\/")[2];
                    CourseID = CourseHTML.substring(0, CourseHTML.lastIndexOf("."));
//                    System.out.println(CourseID);
                }

                return Tuple5.of(data[0], DateUtils.parseMinute(data[1]), CourseID, data[3], data[4]);
            }
        }).filter(new FilterFunction<Tuple5<String, String, String, String, String>>() {
            @Override
            public boolean filter(Tuple5<String, String, String, String, String> value) throws Exception {
                return value.f2 != null;
            }
        });

        /*
        接收器
         */
        CleanData.addSink(new MySQLSink());

        env.execute("Flink kafka");
    }
}