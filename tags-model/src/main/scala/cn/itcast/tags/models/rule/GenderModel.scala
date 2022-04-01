package cn.itcast.tags.models.rule

import cn.itcast.tags.meta.HBaseMeta
import cn.itcast.tags.tools.HBaseTools
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * 315,性别
 * 316,男
 * 317,女
 */
object GenderModel extends Logging {
  def main(args: Array[String]): Unit = {
    // TODO: 1. 创建SparkSession实例对象
    val spark: SparkSession = {
      // 1.a. 创建SparkConf 设置应用信息
      val sparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[4]")
        .set("spark.sql.shuffle.partitions", "4")
        // 由于从HBase表读写数据，设置序列化
        .set("spark.serializer",
          "org.apache.spark.serializer.KryoSerializer")
        .registerKryoClasses(
          Array(classOf[ImmutableBytesWritable],
            classOf[Result], classOf[Put])
        )
      // 1.b. 建造者模式构建SparkSession对象
      val session = SparkSession.builder()
        .config(sparkConf)
        .getOrCreate()
      // 1.c. 返回会话实例对象
      session
    }
    import spark.implicits._
    import org.apache.spark.sql.functions._


    // TODO: 2. 从MySQL数据库读取标签数据（基础标签表：tbl_basic_tag），依据业务标签ID读取
    val tagTable: String =
      """
        |(
        |SELECT `id`,
        | `name`,
        | `rule`,
        | `level`
        |FROM `profile_tags`.`tbl_basic_tag`
        |WHERE id = 315
        |UNION
        |SELECT `id`,
        | `name`,
        | `rule`,
        | `level`
        |FROM `profile_tags`.`tbl_basic_tag`
        |WHERE pid = 315
        |ORDER BY `level` ASC, `id` ASC
        |) AS basic_tag
        |""".stripMargin
    val basicTagDF = spark.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url",
        "jdbc:mysql://bigdata-cdh01.itcast.cn:3306/?" +
          "useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai")
      .option("dbtable", tagTable)
      .option("user", "root")
      .option("password", "123456")
      .load()
    //    basicTagDF.printSchema();
    //    basicTagDF.show(10,false)


    // TODO: 3. 依据业务标签规则获取业务数据，比如到HBase数据库读取表的数据

    // 3.1 4级标签规则rule
    val tagRule: String = basicTagDF
      .filter($"level" === 4)
      .head()
      .getAs[String]("rule")
    // logInfo(s"=========== 业务标签的规则rule：{$tagRule} ===========")

    // 3.2 解析标签规则，先按照换行符\n分割，再按照等号分割
    val ruleMap: Map[String, String] = tagRule
      .split("\\n")
      .map { line =>
        val Array(attrName, attrValue) = line.trim.split("=")
        (attrName, attrValue)
      }
      .toMap
    //logWarning(s"============ { ${ruleMap.mkString(", ")} }===========")

    // 3.3 依据标签规则中inType类型获取数据
    var businessDF: DataFrame = null
    if ("hbase".equals(ruleMap("inType").toLowerCase)) {
      // 规则数据封装到HBaseMeta中
      val hbaseMeta: HBaseMeta = HBaseMeta.getHBaseMeta(ruleMap)
      // 依据条件到HBase表中获取业务数据
      businessDF = HBaseTools.read(
        spark, hbaseMeta.zkHosts, hbaseMeta.zkPort, //
        hbaseMeta.hbaseTable, hbaseMeta.family, //
        hbaseMeta.selectFieldNames.split(",").toSeq //
      )
    } else {
      // 如果未获取到数据，直接抛出异常
      new RuntimeException("业务标签未提供数据源信息，获取不到业务数据，无法计算标签")
    }
//    businessDF.printSchema()
//    businessDF.show(20, truncate = false)


    // TODO: 4. 业务数据和属性标签结合，构建标签：规则匹配型标签 -> rule match

    // 4.1 获取5级标签对应tagRule和tagName
    val attrTagRuleDF: DataFrame = basicTagDF
      .filter($"level" === 5)
      .select($"rule", $"name")
    // 4.2 DataFrame 关联，依据属性标签规则rule与业务数据字段gender
    val modelDF: DataFrame = businessDF
      .join(
        attrTagRuleDF,
        businessDF("gender") === attrTagRuleDF("rule")
      )
      .select(
        $"id".as("userId"), //
        $"name".as("gender") //
      )
//    modelDF.printSchema()
//    modelDF.show(100, truncate = false)
    basicTagDF.unpersist()

    // TODO: 5. 将标签数据存储到HBase表中：用户画像标签表 -> tbl_profile
    HBaseTools.write(
      modelDF, "bigdata-cdh01.itcast.cn", "2181", //
      "tbl_profile", "user", "userId"
    )

    // 应用结束，关闭资源
    spark.stop()
  }
}
