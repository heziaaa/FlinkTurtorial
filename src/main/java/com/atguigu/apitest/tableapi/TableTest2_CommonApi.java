package com.atguigu.apitest.tableapi;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTurtorial
 * Package: com.atguigu.apitest.tableapi
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/10/27 11:35
 */

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @ClassName: TableTest2_CommonApi
 * @Description:
 * @Author: wushengran on 2020/10/27 11:35
 * @Version: 1.0
 */
public class TableTest2_CommonApi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1.1 基于老版本planner的流处理
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, settings);

        // 1.2 基于老版本planner的批处理
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(batchEnv);

        // 1.3 基于blink版本planner的流处理
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnvironment = StreamTableEnvironment.create(env, blinkStreamSettings);

        // 1.4 基于blink版本planner的批处理
        EnvironmentSettings blinkBatchSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();
        TableEnvironment blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings);

        // 2. 连接外部系统，读取数据，创建表

        // 2.1  读取文件
        String filePath = "D:\\Projects\\BigData\\FlinkTurtorial\\src\\main\\resources\\sensor.txt";
        tableEnv.connect( new FileSystem().path(filePath))
                .withFormat( new OldCsv())
                .withSchema(
                        new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable");

        // 3. 查询转换
        // 3.1 简单查询
        // 3.1.1 Table API
        Table inputTable = tableEnv.from("inputTable");
        Table resultTable = inputTable.select("id, temperature")
                .filter("id === 'sensor_1'");

        // 3.1.2 SQL
        Table resultSqlTable = tableEnv.sqlQuery("select id, temperature from inputTable where id = 'sensor_1'");

        // 3.2 聚合统计
        // 3.2.1 Table API
        Table aggTable = inputTable.groupBy("id")
                .select("id, id.count as count");
        // 3.2.2 SQL
        Table aggSqlTable = tableEnv.sqlQuery("select id, count(id) as cnt from inputTable group by id");

        // 4. 转换成流打印输出
        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("resultSQL");
        tableEnv.toRetractStream(aggTable, Row.class).print("agg");
        tableEnv.toRetractStream(aggSqlTable, Row.class).print("aggSQL");

        env.execute();
    }
}
