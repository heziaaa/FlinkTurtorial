package com.atguigu.apitest.processfunction;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTurtorial
 * Package: com.atguigu.apitest.processfunction
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/10/26 14:38
 */

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName: ProcessFuncTest2_KeyedProcessCase
 * @Description:
 * @Author: wushengran on 2020/10/26 14:38
 * @Version: 1.0
 */
public class ProcessFuncTest2_KeyedProcessCase {
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

        // 监控传感器温度值，如果10秒内连续上升则报警
        DataStream<String> warningStream = dataStream.keyBy("id")
                .process( new TempIncreaseWarning(10) );

        warningStream.print();
        env.execute();
    }

    public static class TempIncreaseWarning extends KeyedProcessFunction<Tuple, SensorReading, String>{
        // 定义时间间隔属性
        private Integer interval;

        public TempIncreaseWarning(Integer interval) {
            this.interval = interval;
        }

        // 声明状态
        private ValueState<Double> lastTempState;
        private ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class, Double.MIN_VALUE));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            // 取出上一次的温度值和定时器时间戳
            Double lastTemp = lastTempState.value();
            Long timerTs = timerTsState.value();

            // 更新状态
            lastTempState.update(value.getTemperature());

            // 如果不是第一个数据，而且温度上升，那么就注册一个定时器
            if( timerTs == null && value.getTemperature() > lastTemp ){
                long ts = ctx.timerService().currentProcessingTime() + interval * 1000L;
                ctx.timerService().registerProcessingTimeTimer(ts);
                // 保存定时器时间戳到状态
                timerTsState.update(ts);
            }
            // 如果温度值下降，删除定时器，重新开始
            else if( value.getTemperature() < lastTemp && timerTs != null ){
                ctx.timerService().deleteProcessingTimeTimer(timerTs);
                timerTsState.clear();     // 清空状态
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 如果定时器触发，说明10秒内温度连续上升，报警
            out.collect( "传感器" + ctx.getCurrentKey() + "温度连续" + interval + "秒上升：报警"  );
            timerTsState.clear();
        }
    }
}
