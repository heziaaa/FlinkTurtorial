package com.atguigu.apitest.state;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTurtorial
 * Package: com.atguigu.apitest.state
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/10/26 10:19
 */

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

/**
 * @ClassName: StateTest1_KeyedState
 * @Description:
 * @Author: wushengran on 2020/10/26 10:19
 * @Version: 1.0
 */
public class StateTest1_KeyedState {
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

        dataStream
                .keyBy("id")
                .map( new MyKeyedMapper() )
                .print();

        env.execute();
    }


    // 输出当前数据的count值
    public static class MyMapper implements MapFunction<SensorReading, Integer>, ListCheckpointed<Integer>{
        // 定义一个属性，算子状态
        Integer count = 0;

        @Override
        public Integer map(SensorReading value) throws Exception {
            count++;
            return count;
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(count);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            for( Integer num: state )
                count += num;
        }
    }

    public static class MyKeyedMapper extends RichMapFunction<SensorReading, Integer> {
        // 定义一个状态
        ValueState<Integer> countState;
        ListState<Integer> myListState;
        ReducingState<SensorReading> myReducingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("count", Integer.class, 0));
            myListState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("my-list", Integer.class));
            myReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<SensorReading>("my-reduce", new ReduceFunction<SensorReading>() {
                @Override
                public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                    return new SensorReading(value1.getId(), value2.getTimestamp(), Math.max(value1.getTemperature(), value2.getTemperature()));
                }
            }, SensorReading.class));
        }

        @Override
        public Integer map(SensorReading value) throws Exception {
            Integer count = countState.value();
            count++;
            countState.update(count);
            return count;
        }
    }
}
