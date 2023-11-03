package tech.xuwei.paimon.hivepaimon

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 使用FlinkSQL从Paimon表中读取数据
 * Created by xuwei
 */
object FlinkSQLAlterPaimonTable {
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

    //移除表中的scan.snapshot-id属性
    tEnv.executeSql(
      """
        |ALTER TABLE p_h_t2 RESET('scan.snapshot-id')
        |""".stripMargin)
      .print()

    //查看最新的表属性信息
    tEnv.executeSql(
      """
        |SHOW CREATE TABLE p_h_t2
        |""".stripMargin)
      .print()
  }

}
