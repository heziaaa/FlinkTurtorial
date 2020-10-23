package com.atguigu.apitest.source;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTurtorial
 * Package: com.atguigu.apitest.source
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/10/23 14:21
 */

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * @ClassName: SourceTest4_UDF
 * @Description:
 * @Author: wushengran on 2020/10/23 14:21
 * @Version: 1.0
 */
public class SourceTest4_UDF {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorReading> dataStream = env.addSource( new MySensorSource() );

        dataStream.print();
        env.execute();
    }

    public static class MySensorSource implements SourceFunction<SensorReading> {
        // 运行状态标识位
        private boolean running = true;

        public void run(SourceContext<SensorReading> ctx) throws Exception {
            // 创建一个随机数发生器
            Random random = new Random();

            // 模拟生产实际：有10个传感器，先定义基准温度
            HashMap<String, Double> sensorTempMap = new HashMap<String, Double>();
            for( int i = 0; i < 10; i++ )
                sensorTempMap.put("sensor_" + (i+1), 60 + random.nextGaussian() * 20);

            // 无限循环，随机产生温度传感器数据
            while(running){
                for( String sensorId: sensorTempMap.keySet() ){
                    // 在之前的温度基础上随机波动
                    Double newTemp = sensorTempMap.get(sensorId) + random.nextGaussian();
                    ctx.collect(new SensorReading(sensorId, System.currentTimeMillis(), newTemp));
                    sensorTempMap.put(sensorId, newTemp);
                }
                Thread.sleep(1000L);
            }
        }

        public void cancel() {
            this.running = false;
        }
    }
}
