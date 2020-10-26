package com.atguigu.apitest.state;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTurtorial
 * Package: com.atguigu.apitest.state
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/10/26 11:21
 */

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: StateTest2_AppCase
 * @Description:
 * @Author: wushengran on 2020/10/26 11:21
 * @Version: 1.0
 */
public class StateTest2_AppCase {
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

        // 分组，针对每一个sensor判断前后两次温度差值
        DataStream<Tuple3<String, Double, Double>> warningStream = dataStream.keyBy("id")
                .flatMap( new TempChangeWarning(10.0) );

        warningStream.print();

        env.execute();
    }

    public static class TempChangeWarning extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {
        // 定义私有属性，阈值
        private Double threshold;

        public TempChangeWarning(Double threshold) {
            this.threshold = threshold;
        }

        // 定义状态，保存上一个温度值
        private ValueState<Double> lastTempState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class, Double.MIN_VALUE));
        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            // 获取状态
            Double lastTemp = lastTempState.value();

            // 如果不是第一个值，才做判断
            if( lastTemp != Double.MIN_VALUE ){
                // 判断，如果差值大于阈值，就输出报警
                Double diff = Math.abs(value.getTemperature() - lastTemp);
                if( diff > threshold )
                    out.collect( new Tuple3<String, Double, Double>(value.getId(), lastTemp, value.getTemperature()) );
            }

            // 更新温度状态
            lastTempState.update(value.getTemperature());
        }
    }
}
