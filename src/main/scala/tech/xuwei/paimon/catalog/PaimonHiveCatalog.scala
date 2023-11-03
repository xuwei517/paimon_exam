package tech.xuwei.paimon.catalog

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * Paimon使用Hive Catalog
 * Created by xuwei
 */
object PaimonHiveCatalog {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING)
    val tEnv = StreamTableEnvironment.create(env)

    //创建Paimon类型的Catalog-使用Hive Catalog
    tEnv.executeSql(
      """
        |CREATE CATALOG paimon_hive_catalog WITH(
        |    'type'='paimon',
        |    'metastore' = 'hive',
        |    'uri' = 'thrift://bigdata04:9083',
        |	   'warehouse'='hdfs://bigdata01:9000/paimon'
        |)
        |""".stripMargin)
    tEnv.executeSql("USE CATALOG paimon_hive_catalog")

    //创建Paimon表
    tEnv.executeSql(
      """
        |CREATE TABLE IF NOT EXISTS p_h_t1(
        |    name STRING,
        |	   age INT,
        |    PRIMARY KEY (name) NOT ENFORCED
        |)
        |""".stripMargin)

    //向表中插入数据
    tEnv.executeSql(
      """
        |INSERT INTO p_h_t1(name,age) VALUES('jack',18),('tom',20)
        |""".stripMargin)
  }

}
