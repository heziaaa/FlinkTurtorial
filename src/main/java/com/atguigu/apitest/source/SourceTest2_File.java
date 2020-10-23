package com.atguigu.apitest.source;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTurtorial
 * Package: com.atguigu.apitest.source
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/10/23 11:57
 */

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: SourceTest2_File
 * @Description:
 * @Author: wushengran on 2020/10/23 11:57
 * @Version: 1.0
 */
public class SourceTest2_File {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> dataStream = env.readTextFile("D:\\Projects\\BigData\\FlinkTurtorial\\src\\main\\resources\\sensor.txt");

        dataStream.print();

        env.execute();
    }
}
