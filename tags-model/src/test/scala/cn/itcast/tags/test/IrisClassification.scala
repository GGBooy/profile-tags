package cn.itcast.tags.test

import jodd.util.ThreadUtil.sleep
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{Normalizer, StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object IrisClassification {
  def main(args: Array[String]): Unit = {
    // 构建SparkSession实例对象，通过建造者模式创建
    val spark: SparkSession = {
      SparkSession
        .builder()
        .appName(this.getClass.getSimpleName.stripSuffix("$"))
        .master("local[4]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    }
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    // 自定义Schema信息
    val irisSchema: StructType = StructType(
      Array(
        StructField("id", DoubleType, nullable = true),
        StructField("sepal_length", DoubleType, nullable = true),
        StructField("sepal_width", DoubleType, nullable = true),
        StructField("petal_length", DoubleType, nullable = true),
        StructField("petal_width", DoubleType, nullable = true),
        StructField("category", StringType, nullable = true)
      )
    )
    // 1. 加载数据集，文件属于CSV格式，直接加载
    val rawIrisDF: DataFrame = spark.read
      .schema(irisSchema)
      .option("sep", ",")
      .option("encoding", "UTF-8")
      .option("header", "false")
      .option("inferSchema", "false")
      .csv("datas/iris/iris.data")
    rawIrisDF.printSchema()
    rawIrisDF.show(10, truncate = false)
    // 2. 特征工程
    /*
    1、类别转换数值类型
    类别特征索引化 -> label
    2、组合特征值
    features: Vector
    */
    // 2.1. 类别特征转换StringIndexer
    val indexerModel: StringIndexerModel = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("label")
      .fit(rawIrisDF)
    val df1: DataFrame = indexerModel.transform(rawIrisDF)
    // 2.2. 组合特征值: VectorAssembler
    val assembler: VectorAssembler = new VectorAssembler()
      // 设置特征列名称
      .setInputCols(rawIrisDF.columns.dropRight(1).drop(1))
      .setOutputCol("raw_features")
    val rawFeaturesDF: DataFrame = assembler.transform(df1)
    rawFeaturesDF.printSchema()
    rawFeaturesDF.show(10, truncate = false)
    // 2.3. 特征值正则化，使用L2正则
    val normalizer: Normalizer = new Normalizer()
      .setInputCol("raw_features")
      .setOutputCol("features")
      .setP(2.0)
    val featuresDF: DataFrame = normalizer.transform(rawFeaturesDF)
    //featuresDF.printSchema()
    //featuresDF.show(100, truncate = false)
    // 将数据集缓存，LR算法属于迭代算法，使用多次
    featuresDF.persist(StorageLevel.MEMORY_AND_DISK).count()
    // 3. 使用逻辑回归算法训练模型
    val lr: LogisticRegression = new LogisticRegression()
      // 设置列名称
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      // 设置迭代次数
      .setStandardization(true)
      .setMaxIter(20)
      .setRegParam(0) // 正则化参数
      .setElasticNetParam(0) // 弹性网络参数：L1正则和L2正则联合使用
    // Fit the model
    val lrModel: LogisticRegressionModel = lr.fit(featuresDF)
    // 4. 使用模型预测
    val predictionDF: DataFrame = lrModel.transform(featuresDF)

    featuresDF.show(10, false)
    predictionDF.show(10, false)

    predictionDF
      // 获取真实标签类别和预测标签类别
      .select("label", "prediction")
      .show(150)
    // 5. 模型评估：准确度 = 预测正确的样本数 / 所有的样本数
    import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    println(s"ACCU = ${evaluator.evaluate(predictionDF)}")
    // 6. 模型调优，此处省略
    // 7. 模型保存与加载
    val modelPath =
    s"datas/models/lrModel-${System.currentTimeMillis()}"
    lrModel.save(modelPath)
    val loadLrModel = LogisticRegressionModel.load(modelPath)
    loadLrModel.transform(
      Seq(
        Vectors.dense(Array(5.1, 3.5, 1.4, 0.2))
      )
        .map(x => Tuple1.apply(x))
        .toDF("features")
    ).show(10, truncate = false)
    sleep(1000*60)
    // 应用结束，关闭资源
    spark.stop()
  }
}