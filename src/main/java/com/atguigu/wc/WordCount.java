package com.atguigu.wc;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTurtorial
 * Package: com.atguigu.wc
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/10/21 11:33
 */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @ClassName: WordCount
 * @Description:
 * @Author: wushengran on 2020/10/21 11:33
 * @Version: 1.0
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件读取数据，用一个DataSet来接收
        String inputPath = "D:\\Projects\\BigData\\FlinkTurtorial\\src\\main\\resources\\hello.txt";
        DataSet<String> inputDataSet = env.readTextFile(inputPath);

        // 对每一行数据切分，统计每个单词出现的次数
//        DataSet<Tuple2<String, Integer>> stringTuple2FlatMapOperator = inputDataSet.flatMap(new MyFlatMapper());
//        UnsortedGrouping<Tuple2<String, Integer>> tuple2UnsortedGrouping = stringTuple2FlatMapOperator.groupBy(0);
//        DataSet<Tuple2<String, Integer>> sum = tuple2UnsortedGrouping.sum(1);

        DataSet<Tuple2<String, Integer>> resultDataSet = inputDataSet.flatMap(new MyFlatMapper())
                .groupBy(0)
                .sum(1);

        resultDataSet.print();
    }
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = line.split(" ");
            for( String word: words )
                out.collect( new Tuple2<String, Integer>(word, 1) );
        }
    }
}
