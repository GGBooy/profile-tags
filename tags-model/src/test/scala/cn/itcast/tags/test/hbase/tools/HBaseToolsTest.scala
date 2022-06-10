package cn.itcast.tags.test.hbase.tools
import cn.itcast.tags.tools.HBaseTools
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
object HBaseToolsTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[4]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    import spark.implicits._
    /*
    zkHosts=bigdata-cdh01.itcast.cn
    zkPort=2181
    hbaseTable=tbl_tag_users
    family=detail
    selectFieldNames=id,gender
    */
    read(
      spark, "bigdata-cdh01.itcast.cn", "2181", "tbl_tag_users", "detail", Seq("id", "name")
    )
//    println(df.columns.mkString("Array(", ", ", ")"))
//    println(s"count = ${df.count()}")
//    df.printSchema()
//    df.show(100, truncate = false)

//    HBaseTools.write(df, "bigdata-cdh01.itcast.cn", "2181",
//      "tbl_users", "info", "id"
//    )


    spark.stop()
  }

  def read(spark: SparkSession, zks: String, port: String, table: String,
           family: String, fields: Seq[String]): Unit = {
    /*
    def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
    conf: Configuration = hadoopConfiguration,
    fClass: Class[F],
    kClass: Class[K],
    vClass: Class[V]
    ): RDD[(K, V)]
    */
    // 1. 设置HBase中Zookeeper集群信息
    val conf: Configuration = new Configuration()
    conf.set("hbase.zookeeper.quorum", zks)
    conf.set("hbase.zookeeper.property.clientPort", port.toString)

    import org.apache.hadoop.hbase.client.Connection
    import org.apache.hadoop.hbase.client.ConnectionFactory
    val conn = ConnectionFactory.createConnection(conf)
    import org.apache.hadoop.hbase.TableName
    import org.apache.hadoop.hbase.util.Bytes
    val tablename = TableName.valueOf(Bytes.toBytes(table))
    val myTable = conn.getTable(tablename)
    import org.apache.hadoop.hbase.util.Bytes
    val get = new Get(Bytes.toBytes("row-1"))
    get.addFamily(Bytes.toBytes(family))
    val result = myTable.get(get)


    println(result.toString)
  }
}
