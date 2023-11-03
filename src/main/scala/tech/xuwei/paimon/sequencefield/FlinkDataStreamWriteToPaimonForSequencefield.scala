package tech.xuwei.paimon.sequencefield

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{DataTypes, Schema}
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.types.{Row, RowKind}

/**
 * 使用Flink DataStream API向Paimon表中写入数据
 * Created by xuwei
 */
object FlinkDataStreamWriteToPaimonForSequencefield {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING)
    val tEnv = StreamTableEnvironment.create(env)

    //手工构造一个Changelog DataStream数据流
    val dataStream = env.fromElements(
      //Row.ofKind(RowKind.INSERT, "jack", Int.box(11),null,null,Long.box(1696125662000L)) //+I
      //Row.ofKind(RowKind.INSERT, "jack", Int.box(10),Int.box(175),null,Long.box(1696125660000L)) //+I
      Row.ofKind(RowKind.INSERT, "jack", null,null,"beijing",Long.box(1696125661000L)) //+I
    )(Types.ROW_NAMED(Array("name", "age", "height", "city", "ts_millis"), Types.STRING, Types.INT, Types.INT, Types.STRING,Types.LONG))

    //将DataStream转换为Table
    val schema = Schema.newBuilder()
      .column("name", DataTypes.STRING().notNull()) //主键非空
      .column("age", DataTypes.INT())
      .column("height", DataTypes.INT())
      .column("city", DataTypes.STRING())
      .column("ts_millis", DataTypes.BIGINT())
      .primaryKey("name") //指定主键
      .build()
    val table = tEnv.fromChangelogStream(dataStream, schema, ChangelogMode.all())

    //创建Paimon类型的Catalog
    tEnv.executeSql(
      """
        |CREATE CATALOG paimon_catalog WITH(
        |    'type'='paimon',
        |	   'warehouse'='hdfs://bigdata01:9000/paimon'
        |)
        |""".stripMargin)
    tEnv.executeSql("USE CATALOG paimon_catalog")

    //注册临时表
    tEnv.createTemporaryView("t1",table)


    //创建Paimon类型的表
    tEnv.executeSql(
      """
        |-- 注意：这里的表名使用反引号进行转义，否则会导致SQL DDL语句解析失败
        |CREATE TABLE IF NOT EXISTS `sequence_field`(
        |    name STRING,
        |	   age INT,
        |    height INT,
        |    city STRING,
        |    ts_millis BIGINT,
        |    PRIMARY KEY (name) NOT ENFORCED
        |) WITH (
        |    'changelog-producer' = 'lookup', -- 注意：partial-update表引擎需要和lookup或者full-compaction一起使用时才支持流读
        |    'merge-engine' = 'partial-update',
        |    'partial-update.ignore-delete' = 'true',
        |    'sequence.field' = 'ts_millis',
        |    'sequence.auto-padding' = 'millis-to-micro' -- 将序列字段的精度补足到微妙
        |)
        |""".stripMargin)

    //向Paimon表中写入数据
    tEnv.executeSql(
      """
        |INSERT INTO `sequence_field`
        |SELECT name,age,height,city,ts_millis FROM t1
        |""".stripMargin)



  }

}
