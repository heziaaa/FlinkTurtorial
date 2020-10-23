package com.atguigu.apitest.transform;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTurtorial
 * Package: com.atguigu.apitest.transform
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/10/23 15:48
 */

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: TransformTest3_Reduce
 * @Description:
 * @Author: wushengran on 2020/10/23 15:48
 * @Version: 1.0
 */
public class TransformTest3_Reduce {
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
        } );

        DataStream<SensorReading> resultStream = dataStream.keyBy("id")
                .reduce( new MyReducer() );

        DataStream<SensorReading> resultStream1 = dataStream.keyBy("id")
                .reduce( (currentRes, newValue) -> new SensorReading(
                        currentRes.getId(),
                        newValue.getTimestamp(),
                        Math.max(currentRes.getTemperature(), newValue.getTemperature())) );

        resultStream.print("res");
        resultStream1.print("res1");

        env.execute();
    }

    // 自定义实现ReduceFunction，输出最大温度值和当前时间戳
    public static class MyReducer implements ReduceFunction<SensorReading> {
        @Override
        public SensorReading reduce(SensorReading currentRes, SensorReading newValue) throws Exception {
            return new SensorReading(currentRes.getId(),
                    newValue.getTimestamp(),
                    Math.max(currentRes.getTemperature(), newValue.getTemperature()));
        }
    }
}
