package tech.xuwei.paimon.query

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 批量查询
 * Created by xuwei
 */
object FlinkPaimonBatchQueryForCompact {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //SET 'execution.runtime-mode' = 'batch';
    env.setRuntimeMode(RuntimeExecutionMode.BATCH)//使用批处理模式
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

    //批量查询数据
    tEnv.executeSql(
      """
        |SELECT * FROM query_table_compact
        |/*+ OPTIONS('scan.mode'='compacted-full') */ -- 表需要开启full-compaction，设置changelog-producer和full-compaction.delta-commits
        |""".stripMargin)
      .print()
  }

}
