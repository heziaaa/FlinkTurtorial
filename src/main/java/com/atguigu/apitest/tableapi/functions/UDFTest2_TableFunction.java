package com.atguigu.apitest.tableapi.functions;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTurtorial
 * Package: com.atguigu.apitest.tableapi.functions
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/10/28 10:22
 */

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @ClassName: UDFTest2_TableFunction
 * @Description:
 * @Author: wushengran on 2020/10/28 10:22
 * @Version: 1.0
 */
public class UDFTest2_TableFunction {
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
        Split split = new Split("_");
        tableEnv.registerFunction("split", split);
        Table resultTable = sensorTable
                .joinLateral( "split(id) as (word, length)" )
                .select("id, ts, temperature, word, length");

        // 5.2 SQL
        tableEnv.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnv.sqlQuery(
                "select id, ts, temperature, word, length from sensor, lateral table( split(id) ) as split_id(word, length)"
        );

        tableEnv.toAppendStream(resultTable, Row.class).print();
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");
        env.execute();
    }

    // 自定义实现TableFunction，对String进行拆分，并输出（word，length）
    public static class Split extends TableFunction<Tuple2<String, Integer>>{
        // 属性，分隔符
        private String separator = ",";

        public Split(String separator) {
            this.separator = separator;
        }

        // 必须实现eval方法，做计算输出结果
        public void eval(String str){
            String[] words = str.split(separator);
            for ( String word: words ){
                collect(new Tuple2<>(word, word.length()));
            }
        }
    }
}
