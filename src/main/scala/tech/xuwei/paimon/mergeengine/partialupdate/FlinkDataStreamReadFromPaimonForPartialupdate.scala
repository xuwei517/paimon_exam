package tech.xuwei.paimon.mergeengine.partialupdate

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * 使用Flink DataStream API从Paimon表中读取数据
 * Created by xuwei
 */
object FlinkDataStreamReadFromPaimonForPartialupdate {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val conf = new Configuration()
    //指定WebUI界面的访问端口，默认就是8081
    conf.setString(RestOptions.BIND_PORT,"8081")
    //为了便于在本地通过页面观察任务执行情况，所以开启本地WebUI功能
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING)

    //禁用Chain，把多个算子拆分开单独执行，便于在开发和测试阶段观察，正式执行时不需要禁用chain
    env.disableOperatorChaining()

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

    //执行SQL查询，打印输出结果
    val execSql =
      """
        |SELECT * FROM `merge_engine_partialupdate` -- 此时默认只能查到数据的最新值
        |-- /*+ OPTIONS('scan.mode'='from-snapshot','scan.snapshot-id'='1') */ -- 通过动态表选项来指定数据读取(扫描)模式，以及从哪里开始读取
        |""".stripMargin
    val table = tEnv.sqlQuery(execSql)
    table.execute().print()

  }

}
