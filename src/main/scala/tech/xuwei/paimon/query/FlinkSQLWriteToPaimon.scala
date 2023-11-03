package tech.xuwei.paimon.query

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 使用FlinkSQL向Paimon表中写入数据
 * Created by xuwei
 */
object FlinkSQLWriteToPaimon {

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


    //创建Paimon类型的表
    tEnv.executeSql(
      """
        |CREATE TABLE IF NOT EXISTS `query_table`(
        |    name STRING,
        |	   age INT,
        |    PRIMARY KEY (name) NOT ENFORCED
        |)
        |""".stripMargin)

    //写入数据
    tEnv.executeSql("INSERT INTO query_table(name,age) VALUES('jack',18)")
    tEnv.executeSql("INSERT INTO query_table(name,age) VALUES('tom',19)")
    tEnv.executeSql("INSERT INTO query_table(name,age) VALUES('mick',20)")


  }

}
