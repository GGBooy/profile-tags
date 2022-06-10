package cn.itcast.tags.utils

import cn.itcast.tags.config.ModelConfig
import cn.itcast.tags.tools.HBaseTools
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Date

object SearchUtils {

//  def search(tableName:String, columnName:String, method:Integer, matchValue:String): String = {
//    // 设置Spark应用程序运行的用户：root, 默认情况下为当前系统用户
//    System.setProperty("user.name", ModelConfig.FS_USER)
//    System.setProperty("HADOOP_USER_NAME", ModelConfig.FS_USER)
//    // 声明变量
//    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)
//    import spark.implicits._
//    import org.apache.spark.sql.functions._
//
//
//    var businessDF: DataFrame = null
//
//    if (tableName == "tbl_profile") {
//      businessDF = spark.read
//        .format("hbase")
//        .option("zkHosts", "bigdata-cdh01.itcast.cn")
//        .option("zkPort", "2181")
//        .option("hbaseTable", tableName)
//        .option("family", "user")
//        .option("selectFields", "userID,Name,Sector,Symbol,ratingAgencyName,predictedCredit,currentRatio")
//        .load()
//    } else if (tableName.contains("tbl_tag_corp")) {
//      businessDF = spark.read
//        .format("hbase")
//        .option("zkHosts", "bigdata-cdh01.itcast.cn")
//        .option("zkPort", "2181")
//        .option("hbaseTable", tableName)
//        .option("family", "user")
//        .option("selectFields", "userID,Rating,Name,Symbol,ratingAgencyName,Date,Sector," +
//          "currentRatio,quickRatio,cashRatio,daysOfSalesOutstanding,grossProfitMargin," +
//          "operatingProfitMargin,returnOnEquity,debtEquityRatio,debtRatio,effectiveTaxRate," +
//          "freeCashFlowOperatingCashFlowRatio,cashPerShare,companyEquityMultiplier," +
//          "enterpriseValueMultiple,payablesTurnover")
//        .load()
//    }
//
//
//
//    // 3.4. 返回数据
////    businessDF.printSchema()
//    val path = "resources/result"+ System.currentTimeMillis().toString()
//    businessDF.filter(col(columnName)===matchValue)
//      .coalesce(1).write.option("header", "true")
//      .csv(path)
//
//    path
//  }


  def main(args: Array[String]): Unit = {
    val result = SearchUtils.search("tbl_profile", "Sector", 1, "资本货物")

  }





  def search(tableName:String, columnName:String, method:Integer, matchValue:String): String = {
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

    val businessDF = HBaseTools.read(
      spark, "bigdata-cdh01.itcast.cn", "2181", //
      tableName, "user", //
      "userId,Name,Sector,Symbol,ratingAgencyName,predictedCredit,currentRatio".split(",").toSeq //
    )

    val path = "resources/result"+ System.currentTimeMillis().toString()
    val result = businessDF.filter(col(columnName)===matchValue)
    result.persist()
    result.coalesce(1).write.option("header", "true").csv(path)
    println(path)
    path
  }

}
