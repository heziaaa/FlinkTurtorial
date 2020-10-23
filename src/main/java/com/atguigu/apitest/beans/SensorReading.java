package com.atguigu.apitest.beans;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTurtorial
 * Package: com.atguigu.apitest.beans
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/10/23 11:44
 */

/**
 * @ClassName: SensorReading
 * @Description:
 * @Author: wushengran on 2020/10/23 11:44
 * @Version: 1.0
 */
public class SensorReading {
    // 私有属性
    private String id;
    private Long timestamp;
    private Double temperature;

    // 构造方法
    public SensorReading() {}

    public SensorReading(String id, Long timestamp, Double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public Long getTimestamp() { return timestamp; }
    public void setTimestamp(Long ts) { this.timestamp = ts; }

    public Double getTemperature() { return temperature; }
    public void setTemperature(Double temp) { this.temperature = temp; }

    @Override
    public String toString() {
        return "SensorReading(" +
                id + ", " +
                timestamp + ", " +
                temperature + ")";
    }
}
