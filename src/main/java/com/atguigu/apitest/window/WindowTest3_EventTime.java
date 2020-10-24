package com.atguigu.apitest.window;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTurtorial
 * Package: com.atguigu.apitest.window
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/10/24 16:39
 */

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @ClassName: WindowTest3_EventTime
 * @Description:
 * @Author: wushengran on 2020/10/24 16:39
 * @Version: 1.0
 */
public class WindowTest3_EventTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置事件时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 从socket文本流中读取数据
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        })

        // 设置时间戳和watermark
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTimestamp() * 1000L;
            }
        });

        // 分组开窗聚合，统计15秒内最大温度值以及最近一次数据的时间戳
        DataStream<SensorReading> resultStream = dataStream
                .keyBy("id")
//                .keyBy(SensorReading::getId)
                .timeWindow(Time.seconds(15))
                .reduce(new ReduceFunction<SensorReading>() {
                    @Override
                    public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                        return new SensorReading(value1.getId(), value2.getTimestamp(), Math.max(value1.getTemperature(), value2.getTemperature()));
                    }
                });

        resultStream.print();

        env.execute();
    }
}
