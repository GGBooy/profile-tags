package cn.itcast.tags.test.models.ml

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier, LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{IndexToString, MaxAbsScaler, StringIndexer, StringIndexerModel, VectorAssembler, VectorIndexer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.storage.StorageLevel

object RatingDTModel {
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
      //      .setOutputCol("raw_features")
      .setOutputCol("features")
    val rawFeaturesDF: DataFrame = assembler.transform(df1)
    return rawFeaturesDF
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
    //    featuresDF.printSchema()
    //    featuresDF.select("id", "score", "label", "raw_features", "features")
    //      .show(100, truncate = false)
    featuresDF
  }

  def getDecisionTreeModel(featuresDF: DataFrame, applyDF: DataFrame): PipelineModel = {

    val labelIndexer = new StringIndexer()
      .setInputCol("score")
      .setOutputCol("indexedLabel")
      .fit(featuresDF)

    // Automatically identify categorical features, and index them.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
//      .setMaxCategories(15) // features with > 4 distinct values are treated as continuous.
      .fit(featuresDF)


    // Train a DecisionTree model.
    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    // Chain indexers and tree in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))
    val Array(trainingData, testData) = featuresDF.randomSplit(Array(0.7, 0.3))
    // Train model. This also runs the indexers.
    val model = pipeline.fit(trainingData)

    val prediction1 = model.transform(trainingData)
    val prediction2 = model.transform(testData)
    val prediction3 = model.transform(applyDF)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    prediction1.show(50, false)
    println(s"TRAIN ACCU = ${evaluator.evaluate(prediction1)}")
    prediction2.show(50, false)
    println(s"TEST ACCU = ${evaluator.evaluate(prediction2)}")
    prediction3.show(50, false)
    println(s"APPLY ACCU = ${evaluator.evaluate(prediction3)}")
    val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
    println("Learned classification tree model:\n" + treeModel.toDebugString)

    import prediction1.sparkSession.implicits._
    prediction1.select($"score", $"predictedLabel")
      .write.format("com.databricks.spark.csv")
      .option("header", "true").save("train.csv")
    prediction2.select($"score", $"predictedLabel")
      .write.format("com.databricks.spark.csv")
      .option("header", "true").save("test.csv")
    prediction3.select($"score", $"predictedLabel")
      .write.format("com.databricks.spark.csv")
      .option("header", "true").save("apply.csv")

//    val p = prediction1.select($"predictedLabel")
//    val r = prediction1.select("score")
//    val category = Array("AA(+)", "A", "BBB", "BB", "B", "CCC(-)")
//    val confMatrix = new Array[Array[Int]](6)
//    import scala.collection.JavaConversions._
//

    model


//    // Train a DecisionTree model.
//    val dt = new DecisionTreeClassifier()
//      .setLabelCol("score")
//      .setFeaturesCol("features")
//      .setPredictionCol("prediction")
//
//    // Chain indexers and tree in a Pipeline.
//    val pipeline = new Pipeline()
//      .setStages(Array(dt))
//
//    // Train model. This also runs the indexers.
//    val model = pipeline.fit(featuresDF)
//    val treeModel = model.stages(0).asInstanceOf[DecisionTreeClassificationModel]
//    treeModel
  }



  def getPrediction(myModel: PipelineModel, featuresDF: DataFrame, name: String): Unit = {
    // 4. 使用模型预测
    val predictionDF: DataFrame = myModel.transform(featuresDF)
    predictionDF
      // 获取真实标签类别和预测标签类别
      .select("score", "prediction")
      .show(150)
    //    predictionDF.groupBy("label").count().show()
    // 5. 模型评估：准确度 = 预测正确的样本数 / 所有的样本数
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    println(name + s"ACCU = ${evaluator.evaluate(predictionDF)}")
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
    val DTmodel = getDecisionTreeModel(featuresWashDF, featuresDF)

//    getPrediction(DTmodel, trainSet, "TRAIN")
//    getPrediction(DTmodel, testSet, "TEST")
//    getPrediction(DTmodel, featuresDF, "APPLY")

  }
}
