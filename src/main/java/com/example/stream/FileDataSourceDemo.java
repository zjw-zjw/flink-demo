package com.example.stream;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

/**
 * @Author: Jiewen
 * @Date: 2022-08-01
 */
public class FileDataSourceDemo {
    private static final Logger LOG = LoggerFactory.getLogger(FileDataSourceDemo.class);
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> input = streamEnv.readTextFile("data\\test_1").setParallelism(1);
        SingleOutputStreamOperator<JSONObject> map = input.map((MapFunction<String, JSONObject>) value -> {
            // mac,event_type,count,created_at
            JSONObject jsonObject = JSONObject.parseObject(value);
            return jsonObject;
        });

        SingleOutputStreamOperator<JSONObject> dev2AppStream = map.filter((FilterFunction<JSONObject>) jsonObject -> {
            Object type = jsonObject.get("type");
            return "dev2app".equals(type);
        });

        dev2AppStream.print();

        streamEnv.execute("File DataSource");
    }
}
