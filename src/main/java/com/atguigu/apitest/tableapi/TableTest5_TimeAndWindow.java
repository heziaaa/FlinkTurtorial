package com.atguigu.apitest.tableapi;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTurtorial
 * Package: com.atguigu.apitest.tableapi
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/10/27 16:29
 */

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @ClassName: TableTest5_TimeAndWindow
 * @Description:
 * @Author: wushengran on 2020/10/27 16:29
 * @Version: 1.0
 */
public class TableTest5_TimeAndWindow {
    public static void main(String[] args) throws Exception{
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 读取文件数据
        DataStream<String> inputStream = env.readTextFile("D:\\Projects\\BigData\\FlinkTurtorial\\src\\main\\resources\\sensor.txt");

        // 3. 转换成 POJO
        DataStream<SensorReading> dataStream = inputStream
                .map( line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
                } )
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(SensorReading element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        // 4. 将DataStream转换成Table
//        Table senosrTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature, pt.proctime");
        Table sensorTable = tableEnv.fromDataStream(dataStream, "id, timestamp.rowtime as ts, temperature");

        // 5. 窗口聚合
        // 5.1 Group Windows
        // 5.1.1 Table API
        Table resultTable = sensorTable
                .window(Tumble.over("10.seconds").on("ts").as("tw"))
                .groupBy("id, tw")
                .select("id, id.count, temperature.avg, tw.end");

        // 5.1.2. SQL
        tableEnv.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnv.sqlQuery(
                "select id, count(id) as cnt, avg(temperature) as avgTemp, tumble_end(ts, interval '10' second)" +
                        " from sensor group by id, tumble(ts, interval '10' second) "
        );

//        senosrTable.printSchema();

        tableEnv.toRetractStream(resultTable, Row.class).print();
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");

        env.execute();
    }
}
