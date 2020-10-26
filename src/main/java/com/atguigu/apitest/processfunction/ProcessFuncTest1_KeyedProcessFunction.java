package com.atguigu.apitest.processfunction;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTurtorial
 * Package: com.atguigu.apitest.processfunction
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/10/26 14:18
 */

import com.atguigu.apitest.beans.SensorReading;
import com.atguigu.apitest.state.StateTest2_AppCase;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName: ProcessFuncTest1_KeyedProcessFunction
 * @Description:
 * @Author: wushengran on 2020/10/26 14:18
 * @Version: 1.0
 */
public class ProcessFuncTest1_KeyedProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从socket文本流中读取数据
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 测试KeyedProcessFunction
        dataStream.keyBy("id")
                .process( new MyKeyedProcess() )
                .print();

        env.execute();
    }

    public static class MyKeyedProcess extends KeyedProcessFunction<Tuple, SensorReading, Integer>{
        @Override
        public void processElement(SensorReading value, Context ctx, Collector<Integer> out) throws Exception {
            out.collect(1);    // 主流输出
//            ctx.output();    // 侧输出流

            ctx.timerService().currentProcessingTime();
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000L);
            ctx.timerService().registerEventTimeTimer(ctx.timerService().currentWatermark() + 60000L);
            ctx.timerService().deleteEventTimeTimer(value.getTimestamp() * 1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
            System.out.println("timer " + timestamp);
            out.collect(10);
//            ctx.output();
            ctx.getCurrentKey();
        }
    }
}
