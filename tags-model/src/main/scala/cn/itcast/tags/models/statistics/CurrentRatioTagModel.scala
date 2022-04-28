package cn.itcast.tags.models.statistics

import cn.itcast.tags.models.{AbstractModel, ModelType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{DoubleType, IntegerType}

class CurrentRatioTagModel extends AbstractModel("流动比率", ModelType.MATCH){
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    // 导入隐式转换
    import businessDF.sparkSession.implicits._
    // 导入函数库
    import org.apache.spark.sql.functions._
    // 1. 自定UDF函数，解析分解属性标签的规则rule： 19500101-19591231
    val rule_to_tuple: UserDefinedFunction = udf(
      (rule: String) => {
        val Array(start, end) = rule.split("~").map(_.toDouble)
        // 返回二元组
        (start, end)
      }
    )

    // 2. 获取属性标签数据，解析规则rule
    val attrTagRuleDF: DataFrame = tagDF
      .filter($"level" === 5) // 5级标签
      .select(
        $"name", //
        rule_to_tuple($"rule").as("rules") //
      )
      // 获取起始start和结束end
      .select(
        $"name", //
        $"rules._1".as("start"), //
        $"rules._2".as("end") //
      )
    attrTagRuleDF.printSchema()
    attrTagRuleDF.show(20, true)

    // 3. 业务数据与标签规则关联JOIN，比较范围
    /*
    attrTagDF： attr
    businessDF: business
    SELECT t2.userId, t1.name FROM attr t1 JOIN business t2
    WHERE t1.start <= t2.birthday AND t1.end >= t2.birthday ;
    */
    // 3.1. 转换日期格式： 1982-01-11 -> 19820111
    val currentRatioDF: DataFrame = businessDF
      .select(
        $"id".as("userId"), //
        $"currentRatio".cast(DoubleType)
      )
    currentRatioDF.printSchema()
    currentRatioDF.show(20, true)

    // 3.2. 关联属性规则，设置条件
    val modelDF: DataFrame = currentRatioDF.join(attrTagRuleDF) // 关联
      // 设置关联条件，在... 范围之内
      .where(
        currentRatioDF("currentRatio").>(attrTagRuleDF("start")) &&
        currentRatioDF("currentRatio").<=(attrTagRuleDF("end"))
      )
      // 选取字段
      .select($"userId", $"name".as("currentRatio"))
    modelDF.printSchema()
    modelDF.show(20, truncate = true)
    null
    //    modelDF
  }
}

object CurrentRatioTagModel {
  def main(args: Array[String]): Unit = {
    val tagModel = new CurrentRatioTagModel()
    tagModel.executeModel(406L)
  }
}