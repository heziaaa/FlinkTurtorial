package com.atguigu.apitest.state;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTurtorial
 * Package: com.atguigu.apitest.state
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/10/26 11:49
 */

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: StateTest3_StateBackendAndFaultTolerence
 * @Description:
 * @Author: wushengran on 2020/10/26 11:49
 * @Version: 1.0
 */
public class StateTest3_StateBackendAndFaultTolerence {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 状态后端配置
        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend(""));
        env.setStateBackend(new RocksDBStateBackend(""));

        // 2. 检查点配置
        env.enableCheckpointing(1000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(200);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);

        // 3. 重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));
        env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.minutes(10), Time.seconds(100)));

        // 从socket文本流中读取数据
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        dataStream.print();

        env.execute();
    }
}
