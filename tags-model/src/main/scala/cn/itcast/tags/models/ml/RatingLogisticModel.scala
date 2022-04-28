package cn.itcast.tags.models.ml

import cn.itcast.tags.models.{AbstractModel, ModelType}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{MaxAbsScaler, StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

class RatingLogisticModel extends AbstractModel("Logistic评级", ModelType.ML) {
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    val session: SparkSession = businessDF.sparkSession
    import session.implicits._
    import org.apache.spark.sql.functions._

    var bDF = businessDF

//    bDF.printSchema()
//    bDF.show(20)

    val columns = bDF.columns;
    columns.foreach(column => {
      if (column != "id") {
        bDF = bDF.withColumn(column, col(column).cast(DoubleType))
      }
    })
//    bDF.printSchema()
//    bDF.show(20)
//
//    tagDF.printSchema()
//    tagDF.show(20)


    // 2.1. 类别特征转换StringIndexer
    val indexerModel: StringIndexerModel = new StringIndexer()
      .setInputCol("Rating")
      .setOutputCol("label")
      .fit(bDF)
    val df1: DataFrame = indexerModel.transform(bDF)
    //    df1.printSchema()
    //    df1.filter($"currentRatio" > 1).show(10)

    // 2.2. 组合特征值: VectorAssembler
    val assembler: VectorAssembler = new VectorAssembler()
      // 设置特征列名称
      .setInputCols(businessDF.columns.drop(1))
      .setOutputCol("raw_features")
    //    println("assembler", assembler.toString())
    val rawFeaturesDF: DataFrame = assembler.transform(df1)
    rawFeaturesDF.printSchema()
    rawFeaturesDF.show(10, truncate = false)


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
    featuresDF.printSchema()
    featuresDF.select("id", "score", "label", "raw_features", "features")
      .show(100, truncate = false)



    // 将数据集缓存，LR算法属于迭代算法，使用多次
    featuresDF.persist(StorageLevel.MEMORY_AND_DISK).count()
    // 3. 使用逻辑回归算法训练模型
    val lr: LogisticRegression = new LogisticRegression()
      // 设置列名称
      .setLabelCol("score")
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      // 设置迭代次数
      .setMaxIter(100)
    //      .setRegParam(0.3) // 正则化参数
    //      .setElasticNetParam(0.8) // 弹性网络参数：L1正则和L2正则联合使用

    val Array(trainSet, testSet) = featuresDF.randomSplit(Array(0.8, 0.2))



    // Fit the model
    val lrModel: LogisticRegressionModel = lr.fit(trainSet)
    // 4. 使用模型预测
    val predictionDF: DataFrame = lrModel.transform(trainSet)
    predictionDF
      // 获取真实标签类别和预测标签类别
      .select("score", "prediction")
      .show(150)
    //    predictionDF.groupBy("label").count().show()
    // 5. 模型评估：准确度 = 预测正确的样本数 / 所有的样本数
    import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("score")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    println(s"TRAIN ACCU = ${evaluator.evaluate(predictionDF)}")
    //    // 6. 模型调优，此处省略
    //    // 7. 模型保存与加载
    //    val modelPath = s"datas/models/lrModel-${System.currentTimeMillis()}"
    //    lrModel.save(modelPath)
    //    val loadLrModel = LogisticRegressionModel.load(modelPath)
    //    loadLrModel.transform(
    //      Seq(
    //        Vectors.dense(Array(5.1, 3.5, 1.4, 0.2))
    //      )
    //        .map(x => Tuple1.apply(x))
    //        .toDF("features")
    //    ).show(10, truncate = false)
    //    // 应用结束，关闭资源
    val testResultDF = lrModel.transform(testSet)
    println(s"TEST ACCU = ${evaluator.evaluate(testResultDF)}")





    null
  }

}

object RatingLogisticModel{
  def main(args: Array[String]): Unit = {
    val tagModel = new RatingLogisticModel();
    tagModel.executeModel(412L)
  }
}