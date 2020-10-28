package com.atguigu.apitest.tableapi.functions;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTurtorial
 * Package: com.atguigu.apitest.tableapi.functions
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/10/28 9:44
 */

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

/**
 * @ClassName: UDFTest1_ScalarFunction
 * @Description:
 * @Author: wushengran on 2020/10/28 9:44
 * @Version: 1.0
 */
public class UDFTest1_ScalarFunction {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 读取文件数据
        DataStream<String> inputStream = env.readTextFile("D:\\Projects\\BigData\\FlinkTurtorial\\src\\main\\resources\\sensor.txt");

        // 3. 转换成 POJO
        DataStream<SensorReading> dataStream = inputStream
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
                });

        // 4. 将DataStream转换成Table
        Table sensorTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature");

        // 5. 测试UDF：Scalar Function
        // 5.1 Table API
        HashCode hashCode = new HashCode(23);
        tableEnv.registerFunction("hashCode", hashCode);
        Table resultTable = sensorTable
                .select("id, ts, temperature, hashCode(id)");

        // 5.2 SQL
        tableEnv.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnv.sqlQuery("select id, ts, temperature, hashCode(id) as hc from sensor");

        tableEnv.toAppendStream(resultTable, Row.class).print();
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");
        env.execute();
    }

    // 自定义标量函数，求String的Hash值
    public static class HashCode extends ScalarFunction {
        // 计算因子
        private int factor = 3;

        public HashCode(int factor) {
            this.factor = factor;
        }

        public int eval(String str){
            return str.hashCode() * factor;
        }
    }
}
