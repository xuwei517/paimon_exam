package tech.xuwei.paimon.manage

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 使用FlinkSQL从Paimon表中读取数据
 * Created by xuwei
 */
object FlinkSQLReadTagFromPaimon {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
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

    //读取Paimon表中的数据，并且打印输出结果
    tEnv.executeSql(
      """
        |SELECT * FROM `paimon_catalog`.`default`.`man_manage_tag`
        |/*+ OPTIONS('scan.tag-name'='t-20230101')*/ -- 指定想要查询的标签的名称
        |""".stripMargin)
      .print()
  }

}
