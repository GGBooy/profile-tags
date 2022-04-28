package cn.itcast.tags.models.rule

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame

class SymbolTagModel extends AbstractModel("企业简称", ModelType.MATCH){
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    val modelDF = TagTools.copyTag(businessDF, "Symbol", tagDF)

    modelDF.printSchema()
    modelDF.show(20, true)
    //    modelDF.groupBy("Name").count().show(truncate = true)
//    null
    modelDF
  }
}
object SymbolTagModel{
  def main(args: Array[String]): Unit = {
    val tagModel = new SymbolTagModel()
    tagModel.executeModel(405L)
  }
}