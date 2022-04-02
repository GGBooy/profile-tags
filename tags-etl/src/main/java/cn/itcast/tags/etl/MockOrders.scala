package cn.itcast.tags.etl

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties
import scala.util.Random

object MockOrders {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val ordersDF: DataFrame = spark.read
      .jdbc(
        "jdbc:mysql://bigdata-cdh01.itcast.cn:3306/tags_dat?" +
        "user=root&password=123456&driver=com.mysql.jdbc.Driver",
        "tbl_orders",
        new Properties()
      )

    val user_id_udf: UserDefinedFunction = udf(
      (userId: String) => {
        if(userId.toInt >= 950) {
          val id = new Random().nextInt(950) + 1
          id.toString
        }else{
          userId
        }
      }
    )

    val paycodeList = List("alipay", "wxpay", "chinapay", "cod")
    val pay_code_udf: UserDefinedFunction = udf(
      (paymentcode: String) => {
        if(!paycodeList.contains(paymentcode)){
          val index : Int = new Random().nextInt(4)
          paycodeList(index)
        }else{
          paymentcode
        }
      }
    )

    val payMap: Map[String, String] = Map(
      "alipay" -> "支付宝", "wxpay" -> "微信支付",
      "chinapay" -> "银联支付", "cod" -> "货到付款"
    )
    val pay_name_udf = udf(
      (paymentcode: String) => {
        payMap(paymentcode)
      }
    )


    val newOrdersDF: DataFrame = ordersDF
      .withColumn("memberID", user_id_udf($"memberID"))
      .withColumn("paymentCode", pay_code_udf($"paymentCode"))
      .withColumn("paymentName", pay_name_udf($"paymentCode"))
      .withColumn(
        "finishTime",
        unix_timestamp(
          date_add(from_unixtime($"finishTime"), 350),
          "yyyy-MM-dd"
        )
      )

    newOrdersDF.write
      .mode(SaveMode.Append)
      .jdbc(
        "jdbc:mysql://bigdata-cdh01.itcast.cn:3306/tags_dat?" +
          "user=root&password=123456&driver=com.mysql.jdbc.Driver",
        "tbl_tag_orders",
        new Properties()
      )

    spark.stop()
  }
}
