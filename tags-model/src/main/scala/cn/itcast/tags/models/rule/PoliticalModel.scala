package cn.itcast.tags.models.rule

import cn.itcast.tags.models.BasicModel
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame

/**
 * 标签模型开发：政治面貌标签模型
 */
class PoliticalModel extends BasicModel {
  /*
  331,政治面貌
  332,群众,,1
  333,党员,,2
  334,无党派人士,,3
  */
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    // 计算标签
    val modelDF: DataFrame = TagTools.ruleMatchTag(
      businessDF, "politicalface", tagDF
    )
//    modelDF.printSchema()
//    modelDF.show(100, truncate = false)
    // 返回
    modelDF
  }
}

object PoliticalModel {
  def main(args: Array[String]): Unit = {
    val tagModel = new PoliticalModel()
    tagModel.executeModel(331)
  }
}
