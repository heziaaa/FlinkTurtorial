package com.atguigu.apitest.sink;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTurtorial
 * Package: com.atguigu.apitest.sink
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/10/24 10:21
 */

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @ClassName: SinkTest3_Es
 * @Description:
 * @Author: wushengran on 2020/10/24 10:21
 * @Version: 1.0
 */
public class SinkTest3_Es {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("D:\\Projects\\BigData\\FlinkTurtorial\\src\\main\\resources\\sensor.txt");

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // es的配置项
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("localhost", 9200));

        dataStream.addSink(new ElasticsearchSink.Builder<SensorReading>(httpHosts, new MyEsSinkFunc()).build());

        env.execute();
    }

    public static class MyEsSinkFunc implements ElasticsearchSinkFunction<SensorReading> {
        @Override
        public void process(SensorReading element, RuntimeContext ctx, RequestIndexer indexer) {
            // 包装要写入es的数据，必须是一个Map或者JsonObject
            HashMap<String, String> dataSource = new HashMap<>();
            dataSource.put("id", element.getId());
            dataSource.put("temp", element.getTemperature().toString());
            dataSource.put("ts", element.getTimestamp().toString());

            // 创建index request，用于向es提交数据
            IndexRequest indexRequest = Requests.indexRequest()
                    .index("sensor")
                    .type("readingdata")
                    .source(dataSource);

            // 用indexer发送http请求
            indexer.add(indexRequest);
        }
    }
}
