# 启动 zookeeper
zkserver

# kafka

cd C:\MyProgram\kafka_2.12-2.5.0

## 启动 kafka
.\bin\windows\kafka-server-start.bat .\config\server.properties

## 创建topic
.\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

## 打开一个生产者
.\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic test
### linux下
./kafka-console-producer.sh --broker-list localhost:9092 --topic test
## 打开一个消费者
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test

# 数据格式
1.74.103.143    2018-12-20 18:12:00    "GET /class/130.html HTTP/1.1"    404    https://search.yahoo.com/search?p=Flink实战