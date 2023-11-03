package tech.xuwei.paimon.query

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 批量查询
 * Created by xuwei
 */
object FlinkPaimonBatchQuery {
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
        |SELECT * FROM query_table
        |-- /*+ OPTIONS('scan.mode'='latest-full') */ -- 默认策略，可以省略不写，只读取最新快照中的所有数据
        |-- /*+ OPTIONS('scan.mode'='latest') */ -- 在批处理模式下和latest-full的效果一致
        |-- /*+ OPTIONS('scan.mode'='from-snapshot','scan.snapshot-id'='2') */ -- 只读取指定id的快照中的所有数据
        |-- /*+ OPTIONS('scan.mode'='from-snapshot-full','scan.snapshot-id'='2') */ -- 在批处理模式下和from-snapshot的效果一致
        |-- /*+ OPTIONS('scan.mode'='from-timestamp','scan.timestamp-millis'='1698894440302') */ -- 只读取指定时间戳的快照中的所有数据
        |-- /*+ OPTIONS('scan.mode'='incremental','incremental-between'='1,3') */ -- 指定两个快照id，查询这两个快照之间的增量变化
        |/*+ OPTIONS('scan.mode'='incremental','incremental-between-timestamp'='1698894438731,1698894441669') */ -- 指定两个快照时间戳，查询这两个快照之间的增量变化
        |""".stripMargin)
      .print()
  }

}
