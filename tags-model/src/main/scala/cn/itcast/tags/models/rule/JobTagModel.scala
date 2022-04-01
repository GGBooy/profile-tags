package cn.itcast.tags.models.rule

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame

class JobTagModel extends AbstractModel("职业标签", ModelType.MATCH){
override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
//  businessDF.printSchema()
//  businessDF.show(20, false)
//  tagDF.printSchema()
//  tagDF.show(20, false)
  val modelDF = TagTools.ruleMatchTag(businessDF, "job", tagDF)
  modelDF
  }
}

object JobTagModel {
  def main(args: Array[String]): Unit = {
    val jobTagModel = new JobTagModel()
    jobTagModel.executeModel(324L, isHive = true)
  }
}