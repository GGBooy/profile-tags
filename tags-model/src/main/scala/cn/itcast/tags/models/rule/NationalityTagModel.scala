package cn.itcast.tags.models.rule

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.{HBaseTools, TagTools}
import org.apache.spark.sql.DataFrame

class NationalityTagModel extends AbstractModel("国籍标签", ModelType.MATCH){
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    val modelDF = TagTools.ruleMatchTag(businessDF, "nationality", tagDF)

//    modelDF.printSchema()
//    modelDF.show(20, false)
//    null
    modelDF
  }
}

object NationalityTagModel{
  def main(args: Array[String]): Unit = {
    val tagModel = new NationalityTagModel()
    tagModel.executeModel(335L)
  }
}