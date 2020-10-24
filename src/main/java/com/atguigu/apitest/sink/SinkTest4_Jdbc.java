package com.atguigu.apitest.sink;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTurtorial
 * Package: com.atguigu.apitest.sink
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/10/24 10:46
 */

import com.atguigu.apitest.beans.SensorReading;
import com.atguigu.apitest.source.SourceTest4_UDF;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @ClassName: SinkTest4_Jdbc
 * @Description:
 * @Author: wushengran on 2020/10/24 10:46
 * @Version: 1.0
 */
public class SinkTest4_Jdbc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
//        DataStream<String> inputStream = env.readTextFile("D:\\Projects\\BigData\\FlinkTurtorial\\src\\main\\resources\\sensor.txt");
//
//        // 转换成SensorReading类型
//        DataStream<SensorReading> dataStream = inputStream.map(line -> {
//            String[] fields = line.split(",");
//            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
//        });

        DataStream<SensorReading> dataStream = env.addSource(new SourceTest4_UDF.MySensorSource());

        dataStream.addSink( new MyJdbcSink() );

        env.execute();
    }

    public static class MyJdbcSink extends RichSinkFunction<SensorReading> {
        // 定义连接
        Connection conn = null;
        PreparedStatement insertStmt = null;
        PreparedStatement updateStmt = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 创建连接
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456");
            // 创建预编译语句
            insertStmt = conn.prepareStatement("INSERT INTO sensor_temp (id, temp) VALUES (?, ?)");
            updateStmt = conn.prepareStatement("UPDATE sensor_temp SET temp = ? WHERE id = ?");
        }

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            // 每一条数据都会调用，调用连接执行sql

            // 直接直接update，如果没有更新再直接insert
            updateStmt.setDouble(1, value.getTemperature());
            updateStmt.setString(2, value.getId());
            updateStmt.execute();
            if( updateStmt.getUpdateCount() == 0 ){
                insertStmt.setString(1, value.getId());
                insertStmt.setDouble(2, value.getTemperature());
                insertStmt.execute();
            }
        }

        @Override
        public void close() throws Exception {
            // 清理工作
            insertStmt.close();
            updateStmt.close();
            conn.close();
        }
    }
}
