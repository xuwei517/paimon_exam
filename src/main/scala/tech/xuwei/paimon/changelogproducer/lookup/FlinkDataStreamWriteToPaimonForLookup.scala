package tech.xuwei.paimon.changelogproducer.lookup

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
object FlinkDataStreamWriteToPaimonForLookup {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING)
    val tEnv = StreamTableEnvironment.create(env)

    //手工构造一个Changelog DataStream数据流
    val dataStream = env.fromElements(
      //Row.ofKind(RowKind.INSERT, "jack", Int.box(10)) //+I
      Row.ofKind(RowKind.UPDATE_AFTER, "jack", Int.box(11)) //+U
      //Row.ofKind(RowKind.DELETE, "jack", Int.box(11)) //-D
    )(Types.ROW_NAMED(Array("name", "age"), Types.STRING, Types.INT))

    //将DataStream转换为Table
    val schema = Schema.newBuilder()
      .column("name", DataTypes.STRING().notNull()) //主键非空
      .column("age", DataTypes.INT())
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
        |CREATE TABLE IF NOT EXISTS `changelog_lookup`(
        |    name STRING,
        |	   age INT,
        |    PRIMARY KEY (name) NOT ENFORCED
        |) WITH (
        |    'changelog-producer' = 'lookup'
        |)
        |""".stripMargin)

    //向Paimon表中写入数据
    tEnv.executeSql(
      """
        |INSERT INTO `changelog_lookup`
        |SELECT name,age FROM t1
        |""".stripMargin)



  }

}
