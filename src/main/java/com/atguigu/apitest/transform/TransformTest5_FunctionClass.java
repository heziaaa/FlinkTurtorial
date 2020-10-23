package com.atguigu.apitest.transform;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTurtorial
 * Package: com.atguigu.apitest.transform
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/10/23 16:57
 */

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: TransformTest5_FunctionClass
 * @Description:
 * @Author: wushengran on 2020/10/23 16:57
 * @Version: 1.0
 */
public class TransformTest5_FunctionClass {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("D:\\Projects\\BigData\\FlinkTurtorial\\src\\main\\resources\\sensor.txt");

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        dataStream.map(new MyRichMapper()).print();

        env.execute();
    }

    public static class MyRichMapper extends RichMapFunction<SensorReading, Integer>{
        @Override
        public Integer map(SensorReading value) throws Exception {
            System.out.println("map");
            return getRuntimeContext().getIndexOfThisSubtask();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open");;
        }

        @Override
        public void close() throws Exception {
            System.out.println("close");
        }
    }
}
