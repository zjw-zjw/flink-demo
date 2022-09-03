package com.example.stream.process;

import com.example.stream.entity.Event;
import com.example.stream.source.function.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @Author: Jiewen
 * @Date: 2022-09-03
 */
public class EventTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 处理时间语义，不需要分配时间戳和 watermark
        SingleOutputStreamOperator<Event> stream = env.addSource(new
                CustomSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        // 基于 KeyedStream 定义事件时间定时器
        stream.keyBy(data -> true)
                .process(new KeyedProcessFunction<Boolean, Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        out.collect("数据到达，时间戳为：" + ctx.timestamp());
                        out.collect("数据到达，水位线为： " +
                                ctx.timerService().currentWatermark() + "\n -------分割线-------");
                        // 注册一个10秒的定时器
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 10*1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("定时器触发，触发时间：" + timestamp);
                    }
                })
                .print();

        env.execute();
    }

    public static class CustomSource implements SourceFunction<Event> {

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            // 直接发出测试数据
            ctx.collect(new Event("Mary", "./home", 1000L));
            // 为了更加明显，中间停顿 5 秒钟
            Thread.sleep(5000L);

            // 发出 10 秒后的数据
            ctx.collect(new Event("Mary", "./home", 11000L));
            Thread.sleep(5000L);

            // 发出 10 秒+1ms 后的数据
            ctx.collect(new Event("Alice", "./cart", 11001L));
            Thread.sleep(5000L);
        }

        @Override
        public void cancel() {

        }
    }

    /**
     * 事件时间语义下，定时器触发的条件就是水位线
     * 推进到设定的时间。第一条数据到来后，设定的定时器时间为 1000 + 10 * 1000 = 11000；而当
     * 时间戳为 11000 的第二条数据到来，水位线还处在 999 的位置，当然不会立即触发定时器；而
     * 之后水位线会推进到 10999，同样是无法触发定时器的。必须等到第三条数据到来，将水位线
     * 真正推进到 11000，就可以触发第一个定时器了。第三条数据发出后再过 5 秒，没有更多的数
     * 据生成了，整个程序运行结束将要退出，此时 Flink 会自动将水位线推进到长整型的最大值
     * （Long.MAX_VALUE）。于是所有尚未触发的定时器这时就统一触发了，我们就在控制台看到
     * 了后两个定时器的触发信息。
     */

    /*
        数据到达，时间戳为：1000
        数据到达，水位线为： -9223372036854775808
         -------分割线-------
        数据到达，时间戳为：11000
        数据到达，水位线为： 999
         -------分割线-------
        数据到达，时间戳为：11001
        数据到达，水位线为： 10999
         -------分割线-------
        定时器触发，触发时间：11000
        定时器触发，触发时间：21000
        定时器触发，触发时间：21001
     */
}
