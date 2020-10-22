package com.atguigu.wc;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTurtorial
 * Package: com.atguigu.wc
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/10/21 13:59
 */

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: StreamWordCount
 * @Description:
 * @Author: wushengran on 2020/10/21 13:59
 * @Version: 1.0
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据，用一个DataSet来接收
//        String inputPath = "D:\\Projects\\BigData\\FlinkTurtorial\\src\\main\\resources\\hello.txt";
//        DataStream<String> inputDataStream = env.readTextFile(inputPath);

        // 将数据源换为socket文本流
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        DataStream<String> inputDataStream = env.socketTextStream(host, port);

        // 先切分单词，然后分组统计
        DataStream<Tuple2<String, Integer>> resultDataStream = inputDataStream
                .flatMap(new WordCount.MyFlatMapper()).setParallelism(3)
                .keyBy(0)
                .sum(1);

        resultDataStream.print();

        // 执行任务
        env.execute("my job");
    }
}
