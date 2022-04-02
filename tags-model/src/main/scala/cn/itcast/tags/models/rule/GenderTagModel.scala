package cn.itcast.tags.models.rule

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame

class GenderTagModel extends AbstractModel("性别标签", ModelType.MATCH){
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    val modelDF = TagTools.ruleMatchTag(businessDF, "gender", tagDF)
    modelDF
  }
}

object GenderTagModel {
  def main(args: Array[String]): Unit = {
    new GenderTagModel().executeModel(315L, true)
  }
}