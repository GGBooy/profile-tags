package cn.itcast.tags.test.models.ml

import cn.itcast.tags.tools.HBaseTools
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier, LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{MaxAbsScaler, StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{abs, col, udf}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import scala.math

object RatingLRModel {
  var spark: SparkSession = _

  def Init(): Unit = {
    this.spark = {
      SparkSession
        .builder()
        .appName(this.getClass.getSimpleName.stripSuffix("$"))
        .master("local[3]")
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    }
  }

  def getDataFrame(ruleMap: Map[String, String]): DataFrame = {
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
        case "AA" => 0.9
        case "A" => 0.8
        case "BBB" => 0.7
        case "BB" => 0.6
        case "B" => 0.5
        case "CCC" => 0.4
        case "CC" => 0.4
        case "C" => 0.4
        case "D" => 0.4
//        case "AAA" => 1.0
//        case "AA" => 0.8
//        case "A" => 0.8
//        case "BBB" => 0.7
//        case "BB" => 0.5
//        case "B" => 0.5
//        case "CCC" => 0.3
//        case "CC" => 0.3
//        case "C" => 0.3
//        case "D" => 0.1
      }
      score * 10
    })
    val ScorebnsDF = bnsDF.withColumn("score", addScore(bnsDF("Rating")))
    //    ScorebnsDF.select("id", "Rating", "Score").show(20)

//        val businessDF: DataFrame = ScorebnsDF.select($"id", $"Rating", $"Name", $"Symbol", $"Rating Agency Name", $"Date", $"Sector",
//          $"currentRatio".cast(DoubleType), $"quickRatio".cast(DoubleType), $"cashRatio".cast(DoubleType), $"daysOfSalesOutstanding".cast(DoubleType), $"netProfitMargin".cast(DoubleType),
//          $"pretaxProfitMargin".cast(DoubleType), $"grossProfitMargin".cast(DoubleType), $"operatingProfitMargin".cast(DoubleType), $"returnOnAssets".cast(DoubleType),
//          $"returnOnCapitalEmployed".cast(DoubleType), $"returnOnEquity".cast(DoubleType), $"assetTurnover".cast(DoubleType), $"fixedAssetTurnover".cast(DoubleType),
//          $"debtEquityRatio".cast(DoubleType), $"debtRatio".cast(DoubleType), $"effectiveTaxRate".cast(DoubleType), $"freeCashFlowOperatingCashFlowRatio".cast(DoubleType),
//          $"freeCashFlowPerShare".cast(DoubleType), $"cashPerShare".cast(DoubleType), $"companyEquityMultiplier".cast(DoubleType), $"ebitPerRevenue".cast(DoubleType),
//          $"enterpriseValueMultiple".cast(DoubleType), $"operatingCashFlowPerShare".cast(DoubleType), $"operatingCashFlowSalesRatio".cast(DoubleType),
//          $"payablesTurnover".cast(DoubleType), $"score".cast(DoubleType))

    val businessDF: DataFrame = ScorebnsDF.select($"id", $"Rating", $"Name", $"Symbol", $"ratingAgencyName", $"Date", $"Sector",
      $"currentRatio".cast(DoubleType), $"quickRatio".cast(DoubleType), $"cashRatio".cast(DoubleType), $"daysOfSalesOutstanding".cast(DoubleType),
      $"grossProfitMargin".cast(DoubleType), $"operatingProfitMargin".cast(DoubleType), $"returnOnEquity".cast(DoubleType), $"debtEquityRatio".cast(DoubleType),
      $"debtRatio".cast(DoubleType), $"effectiveTaxRate".cast(DoubleType), $"freeCashFlowOperatingCashFlowRatio".cast(DoubleType), $"cashPerShare".cast(DoubleType),
      $"companyEquityMultiplier".cast(DoubleType), $"enterpriseValueMultiple".cast(DoubleType), $"payablesTurnover".cast(DoubleType), $"score".cast(DoubleType))

//    val businessDF: DataFrame = ScorebnsDF.select($"id", $"Rating", $"Name", $"Symbol", $"Rating Agency Name", $"Date", $"Sector",
//      $"currentRatio".cast(DoubleType), $"quickRatio".cast(DoubleType), $"cashRatio".cast(DoubleType), $"daysOfSalesOutstanding".cast(DoubleType)./(50), $"netProfitMargin".cast(DoubleType).*(10),
//      $"pretaxProfitMargin".cast(DoubleType).*(10), $"grossProfitMargin".cast(DoubleType).*(2), $"operatingProfitMargin".cast(DoubleType).*(4), $"returnOnAssets".cast(DoubleType).*(10),
//      $"returnOnCapitalEmployed".cast(DoubleType).*(5), $"returnOnEquity".cast(DoubleType).*(5), $"assetTurnover".cast(DoubleType), $"fixedAssetTurnover".cast(DoubleType)./(10),
//      $"debtEquityRatio".cast(DoubleType)./(3), $"debtRatio".cast(DoubleType).*(3), $"effectiveTaxRate".cast(DoubleType).*(2), $"freeCashFlowOperatingCashFlowRatio".cast(DoubleType),
//      $"freeCashFlowPerShare".cast(DoubleType)./(8), $"cashPerShare".cast(DoubleType)./(10), $"companyEquityMultiplier".cast(DoubleType)./(2), $"ebitPerRevenue".cast(DoubleType).*(5),
//      $"enterpriseValueMultiple".cast(DoubleType)./(10), $"operatingCashFlowPerShare".cast(DoubleType)./(7), $"operatingCashFlowSalesRatio".cast(DoubleType).*(5),
//      $"payablesTurnover".cast(DoubleType)./(10), $"score".cast(DoubleType))


    val indexerModel: StringIndexerModel = new StringIndexer()
      .setInputCol("Rating")
      .setOutputCol("label")
      .fit(businessDF)
    val df1: DataFrame = indexerModel.transform(businessDF)
    //    df1.printSchema()
    //    df1.filter($"currentRatio" > 1).show(10)

    // 2.2. 组合特征值: VectorAssembler
    val assembler: VectorAssembler = new VectorAssembler()
      // 设置特征列名称
      .setInputCols(businessDF.columns.slice(7, businessDF.columns.length -1))
      .setOutputCol("raw_features")
//      .setOutputCol("features")
    val rawFeaturesDF: DataFrame = assembler.transform(df1)
//    return rawFeaturesDF
    //    rawFeaturesDF.printSchema()
    //    rawFeaturesDF.show(10, truncate = false)

    //    // 2.3. 特征值正则化，使用L2正则
    //    val normalizer: Normalizer = new Normalizer()
    //      .setInputCol("raw_features")
    //      .setOutputCol("features")
    //      .setP(2.0)
    //    val featuresDF: DataFrame = normalizer.transform(rawFeaturesDF)
    //    featuresDF.printSchema()
    //    featuresDF.show(100, truncate = false)


    val scaler = new MaxAbsScaler()
      .setInputCol("raw_features")
      .setOutputCol("features")
    val scalerModel = scaler.fit(rawFeaturesDF)
    val featuresDF = scalerModel.transform(rawFeaturesDF)
//    val featuresDF = rawFeaturesDF.withColumn("features", col("raw_features"))


    //    featuresDF.printSchema()
    //    featuresDF.select("id", "score", "label", "raw_features", "features")
    //      .show(100, truncate = false)
    featuresDF
  }

  def getLogisticModel(featuresDF: DataFrame): LogisticRegressionModel = {
    // 将数据集缓存，LR算法属于迭代算法，使用多次
    featuresDF.persist(StorageLevel.MEMORY_AND_DISK).count()
    // 3. 使用逻辑回归算法训练模型
    val lr: LogisticRegression = new LogisticRegression()
      // 设置列名称
      .setLabelCol("score")
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      // 设置迭代次数
      .setMaxIter(200)
          .setRegParam(0.3) // 正则化参数
//          .setElasticNetParam(0.8) // 弹性网络参数：L1正则和L2正则联合使用

    //    val Array(trainSet, testSet) = featuresDF.randomSplit(Array(0.8, 0.2))


    // Fit the model
    val lrModel: LogisticRegressionModel = lr.fit(featuresDF)
    lrModel
  }

//  def getDecisionTreeModel(featuresDF: DataFrame): DecisionTreeClassificationModel = {
//    val dtClassifier = new DecisionTreeClassifier()
//      .setLabelCol("score")
//      .setFeaturesCol("features")
//      .setPredictionCol("prediction")
//    val dt:DecisionTreeClassificationModel = dtClassifier.fit(featuresDF)
//    dt
//  }

  val transformScore = udf((score: Double, prediction: Double) => {
    var result = 0
    if ((prediction - score)>= -1 && (prediction - score) <= 1){
      result = 0
    } else {
      result = 1
    }
    result*1.0
  })

  def getPrediction(myModel: LogisticRegressionModel, featuresDF: DataFrame, name: String): Unit = {
    // 4. 使用模型预测
    val predictionDF: DataFrame = myModel.transform(featuresDF)
//    predictionDF
//      // 获取真实标签类别和预测标签类别
//      .select("score", "prediction")
//      .show(150)
    //    predictionDF.groupBy("label").count().show()

    val predictionTemp = predictionDF.withColumn("scoreTemp", col("score")*0)
                          .withColumn("predictionTemp", transformScore(col("score"), col("prediction")))
//    predictionTemp.select("score", "prediction", "predictionTemp").show(50)
    predictionTemp.show(10, true)

    // 5. 模型评估：准确度 = 预测正确的样本数 / 所有的样本数
    import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("scoreTemp")
      .setPredictionCol("predictionTemp")
      .setMetricName("accuracy")
    println(name + s"ACCU = ${evaluator.evaluate(predictionTemp)}")
//    import predictionDF.sparkSession.implicits._
//    predictionDF.select($"score", $"prediction")
//      .coalesce(1).write.option("header", "true")
//      .csv("tags-model/src/test/resources/" + name)
  }

  val restoreCredit = udf((score: Double) => {
    var result = ""
    if (math.abs(score - 10) < 0.1){
      result = "AAA"
    }
    if (math.abs(score - 9) < 0.1){
      result = "AA"
    }
    if (math.abs(score - 8) < 0.1){
      result = "A"
    }
    if (math.abs(score - 7) < 0.1){
      result = "BBB"
    }
    if (math.abs(score - 6) < 0.1){
      result = "BB"
    }
    if (math.abs(score - 5) < 0.1){
      result = "B"
    }
    if (math.abs(score - 4) < 0.1){
      result = "CCC"
    }
    result
  })

  def setTag(myModel: LogisticRegressionModel, featuresDF: DataFrame): Unit = {
    import  featuresDF.sparkSession.implicits._

    val predictionDF: DataFrame = myModel.transform(featuresDF).withColumn("predictedCredit", restoreCredit(col("prediction")))
      .select(
      $"id".as("userId"), //
      $"predictedCredit".as("predictedCredit") //
    )
    predictionDF.show(30)
    HBaseTools.write(
      predictionDF, "bigdata-cdh01.itcast.cn", "2181", //
      "tbl_profile", "user", "userId"
    )
  }

  def main(args: Array[String]): Unit = {
    Init()
    val columns = ("id,Rating,Name,Symbol,ratingAgencyName,Date,Sector," +
      "currentRatio,quickRatio,cashRatio,daysOfSalesOutstanding,grossProfitMargin," +
      "operatingProfitMargin,returnOnEquity,debtEquityRatio,debtRatio,effectiveTaxRate," +
      "freeCashFlowOperatingCashFlowRatio,cashPerShare,companyEquityMultiplier," +
      "enterpriseValueMultiple,payablesTurnover")

    val ruleMapWash: Map[String, String] = Map(
      "inType" -> "hbase",
      "zkHosts" -> "bigdata-cdh01.itcast.cn",
      "zkPort" -> "2181",
      "hbaseTable" -> "tbl_tag_corp_wash",
      "family" -> "detail",
      "selectFieldNames" -> columns
    )
    val ruleMap: Map[String, String] = Map(
      "inType" -> "hbase",
      "zkHosts" -> "bigdata-cdh01.itcast.cn",
      "zkPort" -> "2181",
      "hbaseTable" -> "tbl_tag_corp",
      "family" -> "detail",
      "selectFieldNames" -> columns
    )
    val featuresDF = getDataFrame(ruleMap)
    val featuresWashDF = getDataFrame(ruleMapWash)
    val Array(trainSet, testSet) = featuresWashDF.randomSplit(Array(0.8, 0.2))
    val Lmodel = getLogisticModel(trainSet)
//    getPrediction(Lmodel, trainSet, "TRAIN")
//    getPrediction(Lmodel, testSet, "TEST")
//    getPrediction(Lmodel, featuresDF, "APPLY")
    setTag(Lmodel, featuresWashDF)

  }

//  def drop(): Unit = {
//    // 构建SparkSession实例对象，通过建造者模式创建
//    val spark: SparkSession = {
//      SparkSession
//        .builder()
//        .appName(this.getClass.getSimpleName.stripSuffix("$"))
//        .master("local[3]")
//        .config("spark.sql.shuffle.partitions", "3")
//        .getOrCreate()
//    }
//    // For implicit conversions like converting RDDs to DataFrames
//    import spark.implicits._
//    // 自定义Schema信息
//    //    val irisSchema: StructType = StructType(
//    //      Array(
//    //        StructField("sepal_length", DoubleType, nullable = true),
//    //        StructField("sepal_width", DoubleType, nullable = true),
//    //        StructField("petal_length", DoubleType, nullable = true),
//    //        StructField("petal_width", DoubleType, nullable = true),
//    //        StructField("category", StringType, nullable = true)
//    //      )
//    //    )
//    //    // 1. 加载数据集，文件属于CSV格式，直接加载
//    //    val rawIrisDF: DataFrame = spark.read
//    //      .schema(irisSchema)
//    //      .option("sep", ",")
//    //      .option("encoding", "UTF-8")
//    //      .option("header", "false")
//    //      .option("inferSchema", "false")
//    //      .csv("datas/iris/iris.data")
//    //    //rawIrisDF.printSchema()
//    //    //rawIrisDF.show(10, truncate = false)
//
//    val columns = ("id,Rating,Name,Symbol,Rating Agency Name,Date,Sector," +
//      "currentRatio,quickRatio,cashRatio,daysOfSalesOutstanding,netProfitMargin," +
//      "pretaxProfitMargin,grossProfitMargin,operatingProfitMargin,returnOnAssets," +
//      "returnOnCapitalEmployed,returnOnEquity,assetTurnover,fixedAssetTurnover," +
//      "debtEquityRatio,debtRatio,effectiveTaxRate,freeCashFlowOperatingCashFlowRatio," +
//      "freeCashFlowPerShare,cashPerShare,companyEquityMultiplier,ebitPerRevenue," +
//      "enterpriseValueMultiple,operatingCashFlowPerShare,operatingCashFlowSalesRatio," +
//      "payablesTurnover")
//
//    val ruleMap: Map[String, String] = Map(
//      "inType" -> "hbase",
//      "zkHosts" -> "bigdata-cdh01.itcast.cn",
//      "zkPort" -> "2181",
//      "hbaseTable" -> "tbl_tag_corp_wash",
//      "family" -> "detail",
//      "selectFieldNames" -> columns
//    )
//    val bnsDF = spark.read
//      .format("hbase")
//      .option("zkHosts", ruleMap("zkHosts"))
//      .option("zkPort", ruleMap("zkPort"))
//      .option("hbaseTable", ruleMap("hbaseTable"))
//      .option("family", ruleMap("family"))
//      .option("selectFields", ruleMap("selectFieldNames"))
//      .option("inferSchema", true)
//      //      .option("filterConditions", ruleMap("filterConditions"))
//      .load()
//
//    val addScore = udf((Rating: String) => {
//      val score = Rating match {
//        case "AAA" => 1.0
//        case "AA" => 1.0
//        case "A" => 0.8
//        case "BBB" => 0.7
//        case "BB" => 0.6
//        case "B" => 0.5
//        case "CCC" => 0.2
//        case "CC" => 0.2
//        case "C" => 0.2
//        case "D" => 0.2
//      }
//      score * 10
//    })
//    val ScorebnsDF = bnsDF.withColumn("score", addScore(bnsDF("Rating")))
//    ScorebnsDF.select("id", "Rating", "Score").show(20)
//
//    val businessDF: DataFrame = ScorebnsDF.select($"id", $"Rating", $"Name", $"Symbol", $"Rating Agency Name", $"Date", $"Sector",
//      $"currentRatio".cast(DoubleType), $"quickRatio".cast(DoubleType), $"cashRatio".cast(DoubleType), $"daysOfSalesOutstanding".cast(DoubleType)./(50), $"netProfitMargin".cast(DoubleType).*(10),
//      $"pretaxProfitMargin".cast(DoubleType).*(10), $"grossProfitMargin".cast(DoubleType).*(2), $"operatingProfitMargin".cast(DoubleType).*(4), $"returnOnAssets".cast(DoubleType).*(10),
//      $"returnOnCapitalEmployed".cast(DoubleType).*(5), $"returnOnEquity".cast(DoubleType).*(5), $"assetTurnover".cast(DoubleType), $"fixedAssetTurnover".cast(DoubleType)./(10),
//      $"debtEquityRatio".cast(DoubleType)./(3), $"debtRatio".cast(DoubleType).*(3), $"effectiveTaxRate".cast(DoubleType).*(2), $"freeCashFlowOperatingCashFlowRatio".cast(DoubleType),
//      $"freeCashFlowPerShare".cast(DoubleType)./(8), $"cashPerShare".cast(DoubleType)./(10), $"companyEquityMultiplier".cast(DoubleType)./(2), $"ebitPerRevenue".cast(DoubleType).*(5),
//      $"enterpriseValueMultiple".cast(DoubleType)./(10), $"operatingCashFlowPerShare".cast(DoubleType)./(7), $"operatingCashFlowSalesRatio".cast(DoubleType).*(5),
//      $"payablesTurnover".cast(DoubleType)./(10), $"score".cast(DoubleType))
//    businessDF.printSchema()
//    //    businessDF.show(20, true)
//
//
//    //    val corpDF = spark.read
//    //      .format("hbase")
//    //      .option("zkHosts", ruleMap("zkHosts"))
//    //      .option("zkPort", ruleMap("zkPort"))
//    //      .option("hbaseTable", "tbl_tag_corp")
//    //      .option("family", ruleMap("family"))
//    //      .option("selectFields", ruleMap("selectFieldNames"))
//    //      .option("inferSchema", true)
//    //      //      .option("filterConditions", ruleMap("filterConditions"))
//    //      .load()
//    //    val corpScoreDF = corpDF.withColumn("score", addScore(corpDF("Rating")))
//    ////    ScorebnsDF.select("id", "Rating", "Score").show(20)
//    //
//    //    val testDF: DataFrame = corpScoreDF.select($"id", $"Rating", $"Name", $"Symbol", $"Rating Agency Name", $"Date", $"Sector",
//    //      $"currentRatio".cast(DoubleType), $"quickRatio".cast(DoubleType), $"cashRatio".cast(DoubleType), $"daysOfSalesOutstanding".cast(DoubleType), $"netProfitMargin".cast(DoubleType),
//    //      $"pretaxProfitMargin".cast(DoubleType), $"grossProfitMargin".cast(DoubleType), $"operatingProfitMargin".cast(DoubleType), $"returnOnAssets".cast(DoubleType),
//    //      $"returnOnCapitalEmployed".cast(DoubleType), $"returnOnEquity".cast(DoubleType), $"assetTurnover".cast(DoubleType), $"fixedAssetTurnover".cast(DoubleType),
//    //      $"debtEquityRatio".cast(DoubleType), $"debtRatio".cast(DoubleType), $"effectiveTaxRate".cast(DoubleType), $"freeCashFlowOperatingCashFlowRatio".cast(DoubleType),
//    //      $"freeCashFlowPerShare".cast(DoubleType), $"cashPerShare".cast(DoubleType), $"companyEquityMultiplier".cast(DoubleType), $"ebitPerRevenue".cast(DoubleType),
//    //      $"enterpriseValueMultiple".cast(DoubleType), $"operatingCashFlowPerShare".cast(DoubleType), $"operatingCashFlowSalesRatio".cast(DoubleType),
//    //      $"payablesTurnover".cast(DoubleType), $"score".cast(DoubleType))
//
//
//    // 2. 特征工程
//    /*
//    1、类别转换数值类型
//    类别特征索引化 -> label
//    2、组合特征值
//    features: Vector
//    */
//    // 2.1. 类别特征转换StringIndexer
//    val indexerModel: StringIndexerModel = new StringIndexer()
//      .setInputCol("Rating")
//      .setOutputCol("label")
//      .fit(businessDF)
//    val df1: DataFrame = indexerModel.transform(businessDF)
//    //    df1.printSchema()
//    //    df1.filter($"currentRatio" > 1).show(10)
//
//    // 2.2. 组合特征值: VectorAssembler
//    val assembler: VectorAssembler = new VectorAssembler()
//      // 设置特征列名称
//      .setInputCols(businessDF.columns.drop(7))
//      .setOutputCol("raw_features")
//    //    println("assembler", assembler.toString())
//    val rawFeaturesDF: DataFrame = assembler.transform(df1)
//    rawFeaturesDF.printSchema()
//    rawFeaturesDF.show(10, truncate = false)
//
//
//    //    // 2.3. 特征值正则化，使用L2正则
//    //    val normalizer: Normalizer = new Normalizer()
//    //      .setInputCol("raw_features")
//    //      .setOutputCol("features")
//    //      .setP(2.0)
//    //    val featuresDF: DataFrame = normalizer.transform(rawFeaturesDF)
//    //    featuresDF.printSchema()
//    //    featuresDF.show(100, truncate = false)
//
//
//    val scaler = new MaxAbsScaler()
//      .setInputCol("raw_features")
//      .setOutputCol("features")
//    val scalerModel = scaler.fit(rawFeaturesDF)
//    val featuresDF = scalerModel.transform(rawFeaturesDF)
//    featuresDF.printSchema()
//    featuresDF.select("id", "score", "label", "raw_features", "features")
//      .show(100, truncate = false)
//
//
//
//
//
//    // 将数据集缓存，LR算法属于迭代算法，使用多次
//    featuresDF.persist(StorageLevel.MEMORY_AND_DISK).count()
//    // 3. 使用逻辑回归算法训练模型
//    val lr: LogisticRegression = new LogisticRegression()
//      // 设置列名称
//      .setLabelCol("score")
//      .setFeaturesCol("features")
//      .setPredictionCol("prediction")
//      // 设置迭代次数
//      .setMaxIter(100)
//    //      .setRegParam(0.3) // 正则化参数
//    //      .setElasticNetParam(0.8) // 弹性网络参数：L1正则和L2正则联合使用
//
//    val Array(trainSet, testSet) = featuresDF.randomSplit(Array(0.8, 0.2))
//
//
//    // Fit the model
//    val lrModel: LogisticRegressionModel = lr.fit(trainSet)
//    // 4. 使用模型预测
//    val predictionDF: DataFrame = lrModel.transform(trainSet)
////    predictionDF
////      // 获取真实标签类别和预测标签类别
////      .select("score", "prediction")
////      .show(150)
//    //    predictionDF.groupBy("label").count().show()
//    // 5. 模型评估：准确度 = 预测正确的样本数 / 所有的样本数
//    import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
//    val evaluator = new MulticlassClassificationEvaluator()
//      .setLabelCol("score")
//      .setPredictionCol("prediction")
//      .setMetricName("accuracy")
//    println(s"TRAIN ACCU = ${evaluator.evaluate(predictionDF)}")
//    //    // 6. 模型调优，此处省略
//    //    // 7. 模型保存与加载
//    //    val modelPath = s"datas/models/lrModel-${System.currentTimeMillis()}"
//    //    lrModel.save(modelPath)
//    //    val loadLrModel = LogisticRegressionModel.load(modelPath)
//    //    loadLrModel.transform(
//    //      Seq(
//    //        Vectors.dense(Array(5.1, 3.5, 1.4, 0.2))
//    //      )
//    //        .map(x => Tuple1.apply(x))
//    //        .toDF("features")
//    //    ).show(10, truncate = false)
//    //    // 应用结束，关闭资源
//    val testResultDF = lrModel.transform(testSet)
//    println(s"TEST ACCU = ${evaluator.evaluate(testResultDF)}")
//
//    spark.stop()
//  }
}
