package tech.xuwei.paimon.query

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 流式查询
 * Created by xuwei
 */
object FlinkPaimonStreamingQueryForConsumerid {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //SET 'execution.runtime-mode' = 'streaming';
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING)//使用流处理模式
    //注意：在流处理模式中，操作Paimon表时需要开启Checkpoint
    env.enableCheckpointing(5000)
    val tEnv = StreamTableEnvironment.create(env)

    //创建Paimon类型的Catalog
    tEnv.executeSql(
      """
        |CREATE CATALOG paimon_catalog WITH(
        |    'type'='paimon',
        |	   'warehouse'='hdfs://bigdata01:9000/paimon'
        |)
        |""".stripMargin)
    tEnv.executeSql("USE CATALOG paimon_catalog")

    //流式查询数据
    tEnv.executeSql(
      """
        |SELECT * FROM query_table
        |/*+ OPTIONS('consumer-id'='con-1') */ -- 指定消费者id
        |""".stripMargin)
      .print()
  }

}
