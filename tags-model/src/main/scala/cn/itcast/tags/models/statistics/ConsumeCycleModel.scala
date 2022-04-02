package cn.itcast.tags.models.statistics
import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame
/**
 * 标签模型开发：消费周期标签模型
 */
class ConsumeCycleModel extends AbstractModel("消费周期标签",
  ModelType.STATISTICS){
  /*
  347 消费周期
  348 近7天 0-7
  349 近2周 8-14
  350 近1月 15-30
  351 近2月 31-60
  352 近3月 61-90
  353 近4月 91-120
  354 近5月 121-150
  355 近半年 151-180
  */
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    //businessDF.show(10, truncate = false)
    // 导入隐式转换
    import businessDF.sparkSession.implicits._
    import org.apache.spark.sql.functions._
    // 1. 获取属性标签数据，解析规则rule
    val attrTagDF: DataFrame = TagTools.convertTuple(tagDF)
    //attrTagDF.show(20, truncate = false)
    // 2. 订单数据按照会员ID：memberid分组，获取最近一次订单完成时间：finishtime
    val daysDF: DataFrame = businessDF
      // 2.1. 分组，获取最新订单时间，并转换格式
      .groupBy($"memberid") //
      .agg(
        from_unixtime(max($"finishtime")).as("finish_time")
      )
      // 2.2. 计算用户最新订单距今天数
      .select(
        $"memberid".as("userId"), //
        datediff(current_timestamp(), $"finish_time").as("consumer_days")
      )
//    daysDF.sort($"userId").show(50, false)

    // 3. 关联属性标签数据和消费天数数据，加上判断条件，进行打标签
    val modelDF: DataFrame = daysDF
      .join(attrTagDF)
      .where(
        daysDF("consumer_days").between(attrTagDF("start"), attrTagDF("end"))
      )
      .select($"userId", $"name".as("consumercycle"))
//    modelDF.printSchema()
//    modelDF.show(20, truncate = false)
//    null
    // 4. 返回标签数据
    modelDF
  }
}
object ConsumeCycleModel{
  def main(args: Array[String]): Unit = {
    val tagModel = new ConsumeCycleModel()
    tagModel.executeModel(350L)
  }
}