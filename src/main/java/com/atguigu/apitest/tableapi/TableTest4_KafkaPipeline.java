package com.atguigu.apitest.tableapi;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTurtorial
 * Package: com.atguigu.apitest.tableapi
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/10/27 15:18
 */

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;

/**
 * @ClassName: TableTest4_KafkaPipeline
 * @Description:
 * @Author: wushengran on 2020/10/27 15:18
 * @Version: 1.0
 */
public class TableTest4_KafkaPipeline {
    public static void main(String[] args) throws Exception{
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 读取kafka数据
        tableEnv.connect( new Kafka()
                .version("0.11")
                .topic("sensor")
                .property("zookeeper.connect", "localhost:2181")
                .property("bootstrap.servers", "localhost:9092")
        )
                .withFormat( new Csv())
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

        // 4. 输出结果表到kafka
        tableEnv.connect( new Kafka()
                .version("0.11")
                .topic("sinktest")
                .property("zookeeper.connect", "localhost:2181")
                .property("bootstrap.servers", "localhost:9092")
        )
                .withFormat( new Csv())
                .withSchema(
                        new Schema()
                                .field("id", DataTypes.STRING())
                                .field("temperature", DataTypes.DOUBLE())
                )
                .createTemporaryTable("outputTable");

        resultTable.insertInto("outputTable");
//        aggSqlTable.insertInto("outputTable");

        env.execute();
    }
}
