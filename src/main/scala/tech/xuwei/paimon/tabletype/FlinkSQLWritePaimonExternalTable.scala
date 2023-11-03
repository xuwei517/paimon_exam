package tech.xuwei.paimon.tabletype

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 使用FlinkSQL向Paimon外部表中写入数据
 * Created by xuwei
 */
object FlinkSQLWritePaimonExternalTable {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING)
    val tEnv = StreamTableEnvironment.create(env)

    //创建Paimon外部表
    tEnv.executeSql(
      """
        |CREATE TABLE paimon_external_user2(
        |    name STRING,
        |	   age INT,
        |    PRIMARY KEY (name) NOT ENFORCED
        |) WITH (
        |    'connector' = 'paimon',
        |    'path' = 'hdfs://bigdata01:9000/paimon/default.db/user2',
        |    'auto-create' = 'true' -- 如果表目录不存在，则自动创建
        |)
        |""".stripMargin)

    //执行数据写入
    tEnv.executeSql(
      """
        |INSERT INTO paimon_external_user2(name,age) VALUES('jack',18)
        |""".stripMargin)
  }

}
