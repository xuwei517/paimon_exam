package tech.xuwei.paimon.manage

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 手工管理Paimon表标签
 * Created by xuwei
 */
object FlinkSQLManagePaimonTag2 {
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

    //创建Paimon表
    tEnv.executeSql(
      """
        |CREATE TABLE IF NOT EXISTS man_manage_tag(
        |    id INT,
        |    name STRING,
        |    PRIMARY KEY (id) NOT ENFORCED
        |)
        |""".stripMargin)

    //向Paimon表中写入数据
    tEnv.executeSql("INSERT INTO man_manage_tag(id,name)VALUES(1,'jack')")
    tEnv.executeSql("INSERT INTO man_manage_tag(id,name)VALUES(2,'tom')")
    tEnv.executeSql("INSERT INTO man_manage_tag(id,name)VALUES(3,'jessic')")


  }

}
