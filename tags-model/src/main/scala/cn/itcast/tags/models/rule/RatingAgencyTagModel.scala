package cn.itcast.tags.models.rule

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame

class RatingAgencyTagModel extends AbstractModel("评级机构", ModelType.MATCH){
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    val modelDF = TagTools.ruleMatchTag(businessDF, "ratingAgencyName", tagDF)

    modelDF.printSchema()
    modelDF.show(20, true)
    modelDF.groupBy("ratingAgencyName").count().show(truncate = true)
//    null
    modelDF
  }
}
object RatingAgencyTagModel{
  def main(args: Array[String]): Unit = {
    val tagModel = new RatingAgencyTagModel()
    tagModel.executeModel(385L)
  }
}