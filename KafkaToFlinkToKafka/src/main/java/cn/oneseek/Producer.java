package cn.oneseek;

import cn.oneseek.util.JsonData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Producer {
    public static void main(String[] args) throws InterruptedException {
        //配置信息
        Properties props = new Properties();
        //kafka服务器地址
        props.put("bootstrap.servers", "localhost:9092");
        //设置数据key和value的序列化处理类
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);
        //创建生产者实例
        KafkaProducer<String,String> producer = new KafkaProducer<>(props);
        Map<String,String> map = new HashMap<>();
        map.put("单体Json",JsonData.str1);
        map.put("嵌套单体Json",JsonData.str2);
        map.put("数组Json",JsonData.str3);
        map.put("嵌套Json数组",JsonData.str4);
        map.put("变体Json(子Json为单体Json或Json数组)",JsonData.str5);
        int x = 10;
        while(x-->0) {
            Thread.sleep(1000);
            for (Map.Entry<String, String> entry : map.entrySet()) {
                ProducerRecord record = new ProducerRecord<String, String>("test", entry.getValue());
                //发送记录
                producer.send(record);
            }
        }

        producer.close();
    }
}