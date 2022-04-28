package cn.itcast.tags.wash

import cn.itcast.tags.config.ModelConfig
import cn.itcast.tags.meta.HBaseMeta
import cn.itcast.tags.utils.SparkUtils
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}


object RatingAnalysis {
  // 设置Spark应用程序运行的用户：root, 默认情况下为当前系统用户
  System.setProperty("user.name", ModelConfig.FS_USER)
  System.setProperty("HADOOP_USER_NAME", ModelConfig.FS_USER)

  // 声明变量
  var spark: SparkSession = _

  def init(isHive: Boolean): Unit = {
    spark = SparkUtils.createSparkSession(this.getClass, isHive)
  }

  def getBusinessData(ruleMap: Map[String, String]): DataFrame = {
    var businessDF: DataFrame = null
    if ("hbase".equals(ruleMap("inType").toLowerCase())) {
      // 规则数据封装到HBaseMeta中
      val hbaseMeta: HBaseMeta = HBaseMeta.getHBaseMeta(ruleMap)
      // 依据条件到HBase中获取业务数据
      /*businessDF = HBaseTools.read(
        spark, hbaseMeta.zkHosts, hbaseMeta.zkPort,
        hbaseMeta.hbaseTable,
        hbaseMeta.family,
        hbaseMeta.selectFieldNames.split(",").toSeq
      )*/
      businessDF = spark.read
        .format("hbase")
        .option("zkHosts", hbaseMeta.zkHosts)
        .option("zkPort", hbaseMeta.zkPort)
        .option("hbaseTable", hbaseMeta.hbaseTable)
        .option("family", hbaseMeta.family)
        .option("selectFields", hbaseMeta.selectFieldNames)
        .option("filterConditions", hbaseMeta.filterConditions)
        .load()
    } else {
      // 如果未获取到数据，直接抛出异常
      new RuntimeException("业务标签未提供数据源信息，获取不到业务数据，无法计算标签")
    }
    // 3.4. 返回数据
    businessDF
  }


  def calcuParal(ruleMap: Map[String, String]): List[List[Double]] = {
    val bnsDF = spark.read
      .format("hbase")
      .option("zkHosts", ruleMap("zkHosts"))
      .option("zkPort", ruleMap("zkPort"))
      .option("hbaseTable", ruleMap("hbaseTable"))
      .option("family", ruleMap("family"))
      .option("selectFields", ruleMap("selectFieldNames"))
      //      .option("filterConditions", ruleMap("filterConditions"))
      .load()
    import bnsDF.sparkSession.implicits._

    val addScore = udf((Rating: String) => {
      val score = Rating match {
        case "AAA" => 1.0
        case "AA" => 1.0
        case "A" => 0.8
        case "BBB" => 0.7
        case "BB" => 0.6
        case "B" => 0.5
        case "CCC" => 0.2
        case "CC" => 0.2
        case "C" => 0.2
        case "D" => 0.2
      }
      score * 10
    })
    val ScorebnsDF = bnsDF.withColumn("score", addScore(bnsDF("Rating")))
    //    ScorebnsDF.select("id", "Rating", "Score").show(20)

    val df: DataFrame = ScorebnsDF.select(
      $"currentRatio".cast(DoubleType), $"quickRatio".cast(DoubleType), $"cashRatio".cast(DoubleType), $"daysOfSalesOutstanding".cast(DoubleType), $"netProfitMargin".cast(DoubleType),
      $"pretaxProfitMargin".cast(DoubleType), $"grossProfitMargin".cast(DoubleType), $"operatingProfitMargin".cast(DoubleType), $"returnOnAssets".cast(DoubleType),
      $"returnOnCapitalEmployed".cast(DoubleType), $"returnOnEquity".cast(DoubleType), $"assetTurnover".cast(DoubleType), $"fixedAssetTurnover".cast(DoubleType),
      $"debtEquityRatio".cast(DoubleType), $"debtRatio".cast(DoubleType), $"effectiveTaxRate".cast(DoubleType), $"freeCashFlowOperatingCashFlowRatio".cast(DoubleType),
      $"freeCashFlowPerShare".cast(DoubleType), $"cashPerShare".cast(DoubleType), $"companyEquityMultiplier".cast(DoubleType), $"ebitPerRevenue".cast(DoubleType),
      $"enterpriseValueMultiple".cast(DoubleType), $"operatingCashFlowPerShare".cast(DoubleType), $"operatingCashFlowSalesRatio".cast(DoubleType),
      $"payablesTurnover".cast(DoubleType))


    var matrix: List[List[Double]] = List()
    val columns = df.columns

    columns.foreach(column1 => {
      var line: List[Double] = List()
      val c1: RDD[Double] = df.select(df.col(column1)).rdd.map(row => row.getAs[Double](column1))
//      c1.take(10).foreach(
//        item => println(item)
//      )
      println(column1 + "--------------")

      columns.foreach(column2 => {
        print(column2 + " \t")
        val c2: RDD[Double] = df.select(df.col(column2)).rdd.map(row => row.getAs[Double](column2))
        val correlation: Double = Statistics.corr(c1, c2)
        line = line :+ correlation
      })
      matrix = matrix :+ line
      println()
    })
    matrix
  }

  def main(args: Array[String]): Unit = {
    init(true)

    val columns = ("id,Rating,Name,Symbol,Rating Agency Name,Date,Sector," +
      "currentRatio,quickRatio,cashRatio,daysOfSalesOutstanding,netProfitMargin," +
      "pretaxProfitMargin,grossProfitMargin,operatingProfitMargin,returnOnAssets," +
      "returnOnCapitalEmployed,returnOnEquity,assetTurnover,fixedAssetTurnover," +
      "debtEquityRatio,debtRatio,effectiveTaxRate,freeCashFlowOperatingCashFlowRatio," +
      "freeCashFlowPerShare,cashPerShare,companyEquityMultiplier,ebitPerRevenue," +
      "enterpriseValueMultiple,operatingCashFlowPerShare,operatingCashFlowSalesRatio," +
      "payablesTurnover")

    val ruleMap: Map[String, String] = Map(
      "inType" -> "hbase",
      "zkHosts" -> "bigdata-cdh01.itcast.cn",
      "zkPort" -> "2181",
      "hbaseTable" -> "tbl_tag_corp",
      "family" -> "detail",
      "selectFieldNames" -> columns
    )

    var result = calcuParal(ruleMap)
    println(result)


//    val businessDF = getBusinessData(ruleMap)
//    import businessDF.sparkSession.implicits._
//    businessDF.as[(String, Double)].filter($"cashPerShare" < 0)
//    businessDF.filter($"cashPerShare" < 0).describe().show()
//    businessDF.groupBy("Rating").count().show()
  }
}
