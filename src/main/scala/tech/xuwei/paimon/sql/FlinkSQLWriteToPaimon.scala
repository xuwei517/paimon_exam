package tech.xuwei.paimon.sql

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
    //注意：在流处理模式中，操作Paimon表时需要开启Checkpoint
    env.enableCheckpointing(5000)
    val tEnv = StreamTableEnvironment.create(env)

    //创建数据源表-普通表
    //注意：此时这个表是在Flink SQL中默认的Catalog里面创建的
    tEnv.executeSql(
      """
        |CREATE TABLE word_source(
        |    word STRING
        |)WITH(
        |    'connector'='datagen',
        |	   'fields.word.length'='1',
        |	   'rows-per-second'='1'
        |)
        |""".stripMargin)

    //创建Paimon类型的Catalog
    tEnv.executeSql(
      """
        |CREATE CATALOG paimon_catalog WITH(
        |    'type'='paimon',
        |	   'warehouse'='hdfs://bigdata01:9000/paimon'
        |)
        |""".stripMargin)
    tEnv.executeSql("USE CATALOG paimon_catalog")


    //创建目的地表-Paimon表
    tEnv.executeSql(
      """
        |CREATE TABLE IF NOT EXISTS wc_sink_sql(
        |    word STRING,
        |	   cnt BIGINT,
        |    PRIMARY KEY (word) NOT ENFORCED
        |)
        |""".stripMargin)

    //向目的地表中写入数据
    tEnv.executeSql(
      """
        |INSERT INTO `paimon_catalog`.`default`.`wc_sink_sql`
        |SELECT
        |    word,
        |    COUNT(*) AS cnt
        |FROM `default_catalog`.`default_database`.`word_source`
        |GROUP BY word
        |""".stripMargin)


  }

}
