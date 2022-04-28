package cn.itcast.tags.models.rule

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame


class NameTagModel extends AbstractModel("企业名称", ModelType.MATCH){
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    val modelDF = TagTools.copyTag(businessDF, "Name", tagDF)

    modelDF.printSchema()
    modelDF.show(20, true)
//    modelDF.groupBy("Name").count().show(truncate = true)
//        null
    modelDF
  }
}
object NameTagModel{
  def main(args: Array[String]): Unit = {
    val tagModel = new NameTagModel()
    tagModel.executeModel(404L)
  }
}