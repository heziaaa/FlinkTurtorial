package com.atguigu.apitest.tableapi.functions;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTurtorial
 * Package: com.atguigu.apitest.tableapi.functions
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/10/28 10:44
 */

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

/**
 * @ClassName: UDFTest3_AggregateFunction
 * @Description:
 * @Author: wushengran on 2020/10/28 10:44
 * @Version: 1.0
 */
public class UDFTest3_AggregateFunction {
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

        // 5. 测试UDF：Table Function
        // 5.1 Table API
        AvgTemp avgTemp = new AvgTemp();
        tableEnv.registerFunction("avgTemp", avgTemp);
        Table resultTable = sensorTable
                .groupBy( "id" )
                .aggregate( "avgTemp(temperature) as avgTemp" )
                .select("id, avgTemp");

        // 5.2 SQL
        tableEnv.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnv.sqlQuery(
                "select id, avgTemp(temperature) from sensor group by id"
        );

        tableEnv.toRetractStream(resultTable, Row.class).print();
        tableEnv.toRetractStream(resultSqlTable, Row.class).print("sql");
        env.execute();
    }

    // 实现自定义的AggregateFunction
    public static class AvgTemp extends AggregateFunction<Double, Tuple2<Double, Integer>>{
        @Override
        public Double getValue(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }

        public void accumulate( Tuple2<Double, Integer> acc, Double temp ){
            acc.f0 += temp;
            acc.f1 += 1;
        }
    }
}
