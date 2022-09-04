package com.example.stream.multistream;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 实时对账
 *
 * app 的支付操作和第三方的支付操作的一个双流 Join。App 的支付事件和第三方的支付事件将
 * 会互相等待 5 秒钟，如果等不来对应的支付事件，那么就输出报警信息。
 * @Author: Jiewen
 * @Date: 2022-09-04
 */
public class BillCheckExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 来自 app 的支付日志
        SingleOutputStreamOperator<Tuple3<String, String, Long>> appStream = env.fromElements(
                Tuple3.of("order-1", "app", 1000L),
                Tuple3.of("order-2", "app", 2000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                return element.f2;
                            }
                        })
        );

        // 来自第三方平台的支付日志
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> thirdpartyStream = env.fromElements(
                Tuple4.of("order-1", "third-party", "success", 3000L),
                Tuple4.of("order-3", "third-party", "success", 4000L)
//                Tuple4.of("order-2", "third-party", "success", 8000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple4<String, String, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple4<String, String, String, Long> element, long recordTimestamp) {
                                return element.f3;
                            }
                        })
        );

        // 检测同一支付单在两条流中是否匹配，不匹配就报警
        appStream.connect(thirdpartyStream)
                // 把两条流中 key 相同的数据放到了一起，然后针对来源的流再做各自处理
                .keyBy(data -> data.f0, data -> data.f0)
                .process(new OrderMatchResult())
                .print();

        env.execute();
    }

    // 自定义实现 CoProcessFunction
    public static class OrderMatchResult extends CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String> {

        // 定义状态变量，用来保存已经到达的事件
        private ValueState<Tuple3<String, String, Long>> appEventState;
        private ValueState<Tuple4<String, String, String, Long>> thirdpartyEventState;

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("初始化状态state。。。。。。。。。");
            appEventState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple3<String, String, Long>>(
                            "app-event",
                            Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
            );
            thirdpartyEventState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple4<String, String, String, Long>>(
                            "thirdparty-event",
                            Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG))
            );
            super.open(parameters);
        }

        @Override
        public void processElement1(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
            System.out.println("app事件：order:" + value.f0 + " 时间：" + value.f2);
            System.out.println("app当前水位线：" + ctx.timerService().currentWatermark());
            // 看另一条流中事件是否来过
            if (thirdpartyEventState.value() != null) {
                out.collect("对账成功：" + value + "    " + thirdpartyEventState.value());
                // 清空状态
                thirdpartyEventState.clear();
            } else {
                // 更新状态
                appEventState.update(value);
                // 注册一个5秒后的定时器，开始等待另一条流的事件
                ctx.timerService().registerEventTimeTimer(value.f2 + 5000L);
            }
        }

        @Override
        public void processElement2(Tuple4<String, String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
            System.out.println("thirdparty事件：order:" + value.f0 + " 时间：" + value.f3);
            System.out.println("thirdparty当前水位线：" + ctx.timerService().currentWatermark());
            if (appEventState.value() != null) {
                out.collect("对账成功：" + value + "    " + appEventState.value());
                // 清空状态
                appEventState.clear();
            } else {
                // 更新状态
                thirdpartyEventState.update(value);
                // 注册一个5秒后的定时器，开始等待另一条流的事件
                ctx.timerService().registerEventTimeTimer(value.f3 + 5000L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            System.out.println("定时器触发当前水位线：" + ctx.timerService().currentWatermark());
            // 定时器触发，判断状态，如果某个状态不为空，说明另一条流中事件没来
            if (appEventState.value() != null) {
                out.collect("对账失败：" + appEventState.value() + "   " + "第三方支付平台信息未到");
            }

            if (thirdpartyEventState.value() != null) {
                out.collect("对账失败：" + thirdpartyEventState.value() + "    " + "app信息未到");
            }

            appEventState.clear();
            thirdpartyEventState.clear();
        }
    }
}
