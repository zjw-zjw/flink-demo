package com.example.stream.source.function;

import com.example.stream.entity.Event;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

/**
 * @Author: Jiewen
 * @Date: 2022-08-06
 */
public class ParallelCustomSource implements ParallelSourceFunction<Integer> {
    private Boolean running = true;
    private Random random = new Random();

    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
        while (running) {
            ctx.collect(random.nextInt());
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
