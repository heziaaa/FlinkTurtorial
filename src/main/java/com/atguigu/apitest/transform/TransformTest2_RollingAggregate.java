package com.atguigu.apitest.transform;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTurtorial
 * Package: com.atguigu.apitest.transform
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/10/23 15:28
 */

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import sun.management.Sensor;

/**
 * @ClassName: TransformTest2_RollingAggregate
 * @Description:
 * @Author: wushengran on 2020/10/23 15:28
 * @Version: 1.0
 */
public class TransformTest2_RollingAggregate {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("D:\\Projects\\BigData\\FlinkTurtorial\\src\\main\\resources\\sensor.txt");

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map( line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        } );
//        .map( new MyMapper() );

        // 分组取最大温度值
        DataStream<SensorReading> resultStream = dataStream.keyBy("id")
                .maxBy("temperature");

        resultStream.print();

        env.execute();
    }

    public static class MyMapper implements MapFunction<String, SensorReading>{
        @Override
        public SensorReading map(String value) throws Exception {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        }
    }
}
