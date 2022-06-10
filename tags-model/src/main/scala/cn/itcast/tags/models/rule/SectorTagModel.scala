package cn.itcast.tags.models.rule

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame


class SectorTagModel extends AbstractModel("企业类型", ModelType.MATCH){
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    val modelDF = TagTools.ruleMatchTag(businessDF, "Sector", tagDF)

    modelDF.printSchema()
    modelDF.show(10, false)
    modelDF.groupBy("Sector").count().show(truncate = false)
    null
//    modelDF
  }
}
object SectorTagModel{
  def main(args: Array[String]): Unit = {
    val tagModel = new SectorTagModel()
    tagModel.executeModel(391L)
  }
}