package com.atguigu.apitest.window;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTurtorial
 * Package: com.atguigu.apitest.window
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/10/24 14:22
 */

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @ClassName: WindowTest1_TimeWindow
 * @Description:
 * @Author: wushengran on 2020/10/24 14:22
 * @Version: 1.0
 */
public class WindowTest1_TimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 从文件读取数据
//        DataStream<String> inputStream = env.readTextFile("D:\\Projects\\BigData\\FlinkTurtorial\\src\\main\\resources\\sensor.txt");

        // 从socket文本流中读取数据
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 开窗处理，每15秒内温度的最小值
        DataStream<Tuple2<String, Double>> resultStream = dataStream
                .map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(SensorReading value) throws Exception {
                        return new Tuple2<>(value.getId(), value.getTemperature());
                    }
                })
                .keyBy(data -> data.f0)
//                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
//                .window(ProcessingTimeSessionWindows.withGap(Time.minutes(1)))
                .timeWindow(Time.seconds(15))
                .minBy(1);

        resultStream.print();

        env.execute();
    }
}
