package tech.xuwei.paimon.datafile

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 分析Paimon底层文件变化
 * Created by xuwei
 */
object FlinkSQLWritePaimonForDataFileOp {
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
        |CREATE TABLE IF NOT EXISTS data_file_op(
        |    id INT,
        |    code INT,
        |    dt STRING,
        |    PRIMARY KEY (id, dt) NOT ENFORCED
        |) PARTITIONED BY (dt)
        |""".stripMargin)

    //第一次执行-写入数据
    //tEnv.executeSql("INSERT INTO data_file_op(id,code,dt) VALUES(1,1001,'20230101')")

    //第二次执行-写入数据
    /*tEnv.executeSql(
      """
        |INSERT INTO data_file_op(id,code,dt) VALUES
        |(99,1099,'20230101'),
        |(2,1002,'20230102'),
        |(3,1003,'20230103'),
        |(4,1004,'20230104'),
        |(5,1005,'20230105'),
        |(6,1006,'20230106'),
        |(7,1007,'20230107'),
        |(8,1008,'20230108'),
        |(9,1009,'20230109'),
        |(10,1010,'20230110');
        |""".stripMargin)*/

    //第三次执行-删除数据，但是Flink1.15版本不支持DELETE语句，需要借助于paimon-flink-action这个jar包执行delete任务
    //tEnv.executeSql("DELETE FROM data_file_op WHERE dt >= '20230103'")

    //第四次执行-完全压缩表数据(触发full-compaction)，借助于paimon-flink-action这个jar包执行compact任务

    //第五次执行-修改表的属性，快照过期相关属性
    /*tEnv.executeSql(
      """
        |ALTER TABLE data_file_op SET(
        |    'snapshot.time-retained' = '1m',
        |    'snapshot.num-retained.min' = '1',
        |    'snapshot.num-retained.max' = '1'
        |)
        |""".stripMargin)*/

    //第六次执行-触发快照过期，通过写入数据触发
    tEnv.executeSql("INSERT INTO data_file_op(id,code,dt) VALUES(11,1011,'20230111')")

  }

}
