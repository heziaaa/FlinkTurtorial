package com.atguigu.apitest.transform;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTurtorial
 * Package: com.atguigu.apitest.transform
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/10/24 9:06
 */

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: TransformTest6_Shuffle
 * @Description:
 * @Author: wushengran on 2020/10/24 9:06
 * @Version: 1.0
 */
public class TransformTest6_Shuffle {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("D:\\Projects\\BigData\\FlinkTurtorial\\src\\main\\resources\\sensor.txt");

        inputStream.print("input");

        DataStream<String> resultStream = inputStream.shuffle();
        resultStream.print("shuffle");

        env.execute();
    }
}
