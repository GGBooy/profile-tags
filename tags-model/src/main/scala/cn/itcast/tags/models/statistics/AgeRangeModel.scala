package cn.itcast.tags.models.statistics

import cn.itcast.tags.models.{AbstractModel, ModelType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.IntegerType

class AgeRangeModel extends AbstractModel("年龄段标签", ModelType.STATISTICS) {
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    /*
    341,年龄段
    342,50后,,19500101-19591231
    343,60后,,19600101-19691231
    344,70后,,19700101-19791231
    345,80后,,19800101-19891231
    346,90后,,19900101-19991231
    347,00后,,20000101-20091231
    348,10后,,20100101-20191231
    349,20后,,20200101-20291231
    * */
    // 导入隐式转换
    import businessDF.sparkSession.implicits._
    // 导入函数库
    import org.apache.spark.sql.functions._
    // 1. 自定UDF函数，解析分解属性标签的规则rule： 19500101-19591231
    val rule_to_tuple: UserDefinedFunction = udf(
      (rule: String) => {
        val Array(start, end) = rule.split("-").map(_.toInt)
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

    // 3. 业务数据与标签规则关联JOIN，比较范围
    /*
    attrTagDF： attr
    businessDF: business
    SELECT t2.userId, t1.name FROM attr t1 JOIN business t2
    WHERE t1.start <= t2.birthday AND t1.end >= t2.birthday ;
    */
    // 3.1. 转换日期格式： 1982-01-11 -> 19820111
    val birthdayDF: DataFrame = businessDF
      .select(
        $"id".as("userId"), //
        regexp_replace($"birthday", "-", "") //
          .cast(IntegerType).as("bornDate")
      )
    // 3.2. 关联属性规则，设置条件
    val modelDF: DataFrame = birthdayDF.join(attrTagRuleDF) // 关联
      // 设置关联条件，在... 范围之内
      .where(
        birthdayDF("bornDate")
          .between(attrTagRuleDF("start"), attrTagRuleDF("end"))
      )
      // 选取字段
      .select($"userId", $"name".as("agerange"))
    //    modelDF.printSchema()
    //    modelDF.show(20, truncate = false)
    //    null
    modelDF
  }
}

object AgeRangeModel {
  def main(args: Array[String]): Unit = {
    val tagModel = new AgeRangeModel()
    tagModel.executeModel(341L)
  }
}
