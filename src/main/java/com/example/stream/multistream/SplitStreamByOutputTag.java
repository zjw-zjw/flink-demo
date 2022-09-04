package com.example.stream.multistream;

import com.example.stream.entity.Event;
import com.example.stream.source.function.ClickSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author: Jiewen
 * @Date: 2022-09-04
 */
public class SplitStreamByOutputTag {

    // 定义输出标签，侧输出流的数据类型为三元组(user, url, timestamp)
    private static OutputTag<Tuple3<String, String, Long>> MaryTag = new
            OutputTag<Tuple3<String, String, Long>>("Mary-pv"){};
    private static OutputTag<Tuple3<String, String, Long>> BobTag = new
            OutputTag<Tuple3<String, String, Long>>("Bob-pv"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env
                .addSource(new ClickSource());

        SingleOutputStreamOperator<Event> processedStream = stream.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event value, Context ctx, Collector<Event> out) throws Exception {
                if (value.user.equals("Mary")) {
                    ctx.output(MaryTag, new Tuple3<>(value.user, value.url, value.timestamp));
                } else if (value.user.equals("Bob")) {
                    ctx.output(BobTag, new Tuple3<>(value.user, value.url, value.timestamp));
                } else {
                    out.collect(value);
                }
            }
        });

        DataStream<Tuple3<String, String, Long>> maryStream = processedStream.getSideOutput(MaryTag);
        DataStream<Tuple3<String, String, Long>> bobStream = processedStream.getSideOutput(BobTag);

        maryStream.print("mary");
        bobStream.print("bob");
        processedStream.print("else");

        env.execute();
    }
}
