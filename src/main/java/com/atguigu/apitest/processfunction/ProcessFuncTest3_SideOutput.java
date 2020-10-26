package com.atguigu.apitest.processfunction;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTurtorial
 * Package: com.atguigu.apitest.processfunction
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/10/26 15:26
 */

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @ClassName: ProcessFuncTest3_SideOutput
 * @Description:
 * @Author: wushengran on 2020/10/26 15:26
 * @Version: 1.0
 */
public class ProcessFuncTest3_SideOutput {
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

        // 定义一个测输出流标签，表示低温流
        final OutputTag<SensorReading> lowTempTag = new OutputTag<SensorReading>("lowTemp"){};

        // 以30度为界，划分高温流和低温流
        SingleOutputStreamOperator<SensorReading> highTempStream = dataStream
                .process(new ProcessFunction<SensorReading, SensorReading>() {
                    @Override
                    public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
                        if (value.getTemperature() > 30)
                            out.collect(value);
                        else
                            ctx.output(lowTempTag, value);
                    }
                });

        DataStream<SensorReading> lowTempStream = highTempStream.getSideOutput(lowTempTag);

        highTempStream.print("high");
        lowTempStream.print("low");

        env.execute();
    }
}
