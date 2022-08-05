package com.example.stream.source;

import com.example.stream.entity.Event;
import com.example.stream.source.function.ClickSource;
import com.example.stream.source.function.ParallelCustomSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: Jiewen
 * @Date: 2022-08-05
 */
public class SourceCustomTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(4);

        // 单并行度
//        DataStreamSource<Event> customSource = streamEnv.addSource(new ClickSource());

        // 并行数据源
        DataStreamSource<Integer> customSource = streamEnv.addSource(new ParallelCustomSource()).setParallelism(2);

        customSource.print();

        streamEnv.execute();
    }
}
