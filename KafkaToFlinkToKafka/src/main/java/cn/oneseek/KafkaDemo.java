package cn.oneseek;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaDemo {
    static final Logger logger = LoggerFactory.getLogger(KafkaDemo.class);

    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //默认情况下，检查点被禁用。要启用检查点，请在StreamExecutionEnvironment上调用enableCheckpointing(n)方法，
        // 其中n是以毫秒为单位的检查点间隔。每隔5000 ms进行启动一个检查点,则下一个检查点将在上一个检查点完成后5秒钟内启动
        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");//kafka的节点的IP或者hostName，多个使用逗号分隔
        properties.setProperty("zookeeper.connect", "localhost:2181");//zookeeper的节点的IP或者hostName，多个使用逗号进行分隔
        properties.setProperty("group.id", "test-consumer-group");//flink consumer flink的消费者的group.id


        FlinkKafkaConsumer011<String> myConsumer = new FlinkKafkaConsumer011<String>(
                "test",
                new SimpleStringSchema(),
                properties);//test0是kafka中开启的topic

        myConsumer.setStartFromLatest();

//        myConsumer.assignTimestampsAndWatermarks(new CustomWatermarkEmitter());
        DataStream<String> keyedStream = env.addSource(myConsumer);//将kafka生产者发来的数据进行处理，本例子未进行任何处理
        logger.info("将kafka生产者发来的数据进行处理，本例子未进行任何处理");
//        keyedStream.print();//直接将从生产者接收到的数据在控制台上进行打印


        DataStream<String> CleanData =
                keyedStream
                        .map(new MapFunction<String, String>() {
                            @Override
                            public String map(String s) throws Exception {

                                return s;
                            }
                        });

        CleanData.print();

        FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<>(
                "localhost:9092",
                "test1",
                new SimpleStringSchema());

        myProducer.setWriteTimestampToKafka(true);

        keyedStream.addSink(myProducer);

        // execute program
        env.execute("Flink Streaming Java API Skeleton");

    }
}
