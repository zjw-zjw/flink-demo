package com.example.stream.source;

import com.example.stream.entity.Event;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @Author: Jiewen
 * @Date: 2022-08-05
 */
public class SourceTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        // 1.从文件读取
        DataStreamSource<String> textFile = streamEnv.readTextFile("data/clicks.txt");

        // 2.从集合中读取
        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary", "./home", 1000L));
        events.add(new Event("Tom", "./home", 2000L));
        events.add(new Event("Bob", "./cart", 3000L));
        DataStreamSource<Event> eventStream = streamEnv.fromCollection(events);

        // 3.从元素读取数据
        DataStreamSource<Event> eventStream2 = streamEnv.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Tom", "./home", 2000L),
                new Event("Bob", "./cart", 3000L)
        );

        // 4.从socket文本流读取
        DataStreamSource<String> socketTextStream = streamEnv.socketTextStream("localhost", 9999);

        // 5.从kafka中读取数据
        String topic = "clicks";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.224.11:9092");
        properties.setProperty("group.id", "flink-test");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("auto.offset.reset", "latest");
        DataStreamSource<String> kafkaStream = streamEnv.addSource(new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties));

        kafkaStream.print("kafka");

//        socketTextStream.print();
//        eventStream.print("events");
//        eventStream2.print("events-2");
//        textFile.print("text");

        streamEnv.execute();
    }
}
