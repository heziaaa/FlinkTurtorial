package com.atguigu.apitest.tableapi;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTurtorial
 * Package: com.atguigu.apitest.tableapi
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/10/27 14:33
 */

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;

/**
 * @ClassName: TableTest3_FileOutput
 * @Description:
 * @Author: wushengran on 2020/10/27 14:33
 * @Version: 1.0
 */
public class TableTest3_FileOutput {
    public static void main(String[] args) throws Exception{
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 读取文件数据
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

        // 4. 输出结果表到文件
        String outputPath = "D:\\Projects\\BigData\\FlinkTurtorial\\src\\main\\resources\\output.txt";
        tableEnv.connect( new FileSystem().path(outputPath))
                .withFormat( new OldCsv())
                .withSchema(
                        new Schema()
                                .field("id", DataTypes.STRING())
                                .field("temp", DataTypes.DOUBLE())
//                                .field("count", DataTypes.BIGINT())
                )
                .createTemporaryTable("outputTable");

        resultTable.insertInto("outputTable");
//        aggSqlTable.insertInto("outputTable");

        env.execute();
    }
}
