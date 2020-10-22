import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTurtorial
  * Package: 
  * Version: 1.0
  *
  * Created by wushengran on 2020/10/21 14:29
  */
object StreamWordCountInScala {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val paramTool = ParameterTool.fromArgs(args)
    val host = paramTool.get("host")
    val port = paramTool.getInt("port")

    // 接收socket文本流
    val inputDataStream: DataStream[String] = env.socketTextStream(host, port)
    val resultDataStream: DataStream[(String, Int)] = inputDataStream
      .flatMap( _.split(" ") )
      .map( (_, 1) )
      .keyBy(0)
      .sum(1)

    resultDataStream.print()

    env.execute()
  }
}
