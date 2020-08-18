package kafka;

import utils.JsonData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Producer {
    public static void main(String[] arg) {
        run("tailingsAlarm");
    }
    public static void run(String topic){
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

        map.put("单体Json",JsonData.单体json); // 支持
//        map.put("嵌套单体Json",JsonData.嵌套单体json); // 支持
//        map.put("数组Json",JsonData.数组json); //不支持
//        map.put("嵌套json数组",JsonData.嵌套json数组); // 支持
//        map.put("变体Json(子Json为单体Json)",JsonData.变体json_子json为单体); // 支持
//        map.put("变体json为Json数组",JsonData.变体json_子json为Json数组); //不支持

        for (Map.Entry<String,String> entry:map.entrySet()){
            ProducerRecord record = new ProducerRecord<String, String>(topic, entry.getValue());
            //发送记录
            producer.send(record);
        }

        producer.close();
    }
}