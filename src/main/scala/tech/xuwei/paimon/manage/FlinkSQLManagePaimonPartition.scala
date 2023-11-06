package tech.xuwei.paimon.manage

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 创建Paimon分区表
 * Created by xuwei
 */
object FlinkSQLManagePaimonPartition {
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

    //创建Paimon分区表
    tEnv.executeSql(
      """
        |CREATE TABLE IF NOT EXISTS man_manage_par(
        |    id INT,
        |    name STRING,
        |    dt STRING,
        |    hh STRING,
        |    PRIMARY KEY (id, dt, hh) NOT ENFORCED
        |) PARTITIONED BY (dt, hh)
        |""".stripMargin)

    //向Paimon分区表中写入数据
    tEnv.executeSql("INSERT INTO man_manage_par(id,name,dt,hh)VALUES(1,'jack','20230101','10')")
    tEnv.executeSql("INSERT INTO man_manage_par(id,name,dt,hh)VALUES(2,'tom','20230101','11')")
    tEnv.executeSql("INSERT INTO man_manage_par(id,name,dt,hh)VALUES(3,'mick','20230102','12')")
    tEnv.executeSql("INSERT INTO man_manage_par(id,name,dt,hh)VALUES(4,'jessic','20230102','13')")
  }

}
