package com.example.stream.source.function;

import com.example.stream.entity.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @Author: Jiewen
 * @Date: 2022-08-05
 */
public class ClickSource implements SourceFunction<Event> {
    // 声明一个标志位
    private Boolean running = true;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {

        // 随机生产数据
        Random random = new Random();
        // 定义字段选取的数据集
        String[] users = {"Mary", "Tom", "Bob"};
        String[] urls = {"./home", "./cart", "./product?id=101", "./product?id=111"};

        // 循环生产数据
        while (running) {
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            Long timestamp = System.currentTimeMillis();
            ctx.collect(new Event(user, url, timestamp));
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
