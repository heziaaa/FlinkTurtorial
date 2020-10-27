package com.atguigu.apitest.tableapi;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTurtorial
 * Package: com.atguigu.apitest.tableapi
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/10/27 10:54
 */

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http2.Http2Exception;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @ClassName: TableTest1_SimpleExample
 * @Description:
 * @Author: wushengran on 2020/10/27 10:54
 * @Version: 1.0
 */
public class TableTest1_SimpleExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("D:\\Projects\\BigData\\FlinkTurtorial\\src\\main\\resources\\sensor.txt");

        // 2. 转换成 POJO
        DataStream<SensorReading> dataStream = inputStream
                .map( line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
                } );

        // 3. 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 4. 基于DataStream创建表
        Table dataTable = tableEnv.fromDataStream(dataStream);

        // 5. 简单转换
        Table resultTable = dataTable.select("id, temperature")
                .where("id = 'sensor_1'");

        // SQL
        tableEnv.createTemporaryView("sensor", dataTable);
        String sql = "select id, temperature from sensor where id = 'sensor_1'";
        Table sqlResultTable = tableEnv.sqlQuery(sql);

        // 6. 转换成流打印输出
        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toAppendStream(sqlResultTable, Row.class).print("sql");

        env.execute();
    }
}
