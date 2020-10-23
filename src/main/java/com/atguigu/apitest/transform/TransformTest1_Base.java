package com.atguigu.apitest.transform;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTurtorial
 * Package: com.atguigu.apitest.transform
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/10/23 14:46
 */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: TransformTest1_Base
 * @Description:
 * @Author: wushengran on 2020/10/23 14:46
 * @Version: 1.0
 */
public class TransformTest1_Base {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> dataStream = env.readTextFile("D:\\Projects\\BigData\\FlinkTurtorial\\src\\main\\resources\\sensor.txt");

        // map转换
        dataStream.map(new MapFunction<String, Integer>() {
            public Integer map(String value) throws Exception {
                return value.length();
            }
        })
        .print("map");
//        dataStream.map( line -> line.length() ).print();

        // flatmap
        dataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] fileds = value.split(",");
                for( String filed: fileds )
                    out.collect(filed);
            }
        }).print("flatmap");

        // filter
        dataStream.filter( line -> line.startsWith("sensor_1") )
        .print("filter");

        env.execute();
    }
}
