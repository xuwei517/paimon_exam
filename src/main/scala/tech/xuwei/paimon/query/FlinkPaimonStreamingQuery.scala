package tech.xuwei.paimon.query

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 流式查询
 * Created by xuwei
 */
object FlinkPaimonStreamingQuery {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //SET 'execution.runtime-mode' = 'streaming';
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING)//使用流处理模式
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
        |-- /*+ OPTIONS('scan.mode'='latest-full') */ -- 默认策略，可以省略不写，第一次启动时读取最新快照中的所有数据，然后继续读取后续新增的变更数据
        |-- /*+ OPTIONS('scan.mode'='latest') */ -- 只读取最新的变更数据
        |-- /*+ OPTIONS('scan.mode'='from-snapshot','scan.snapshot-id'='2') */ -- 从指定id的快照开始读取变更数据(包含后续新增)
        |-- /*+ OPTIONS('scan.mode'='from-snapshot-full','scan.snapshot-id'='2') */ -- 第一次启动时读取指定id的快照中的所有数据，然后继续读取后续新增的变更数据
        |/*+ OPTIONS('scan.mode'='from-timestamp','scan.timestamp-millis'='1698894440302') */ -- 从指定时间戳的快照开始读取变更数据(包含后续新增)
        |""".stripMargin)
      .print()
  }

}
