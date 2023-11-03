package tech.xuwei.paimon.tabletype

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 使用FlinkSQL从Paimon分区表中读取数据
 * Created by xuwei
 */
object FlinkSQLReadPaimonPartitionTable {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING)
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

    //执行查询，并且打印输出结果
    tEnv.executeSql(
      """
        |SELECT
        |    *
        |FROM `paimon_catalog`.`default`.`user_par`
        |WHERE dt = '20230101' AND hh in ('10','11')
        |""".stripMargin)
      .print()
  }

}
