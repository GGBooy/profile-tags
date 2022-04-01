package cn.itcast.tags.test.hbase.write

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

object MyWriteTest {
  def main(args: Array[String]): Unit = {
    val put = new Put(Bytes.toBytes(123456))
    put.addColumn(
      Bytes.toBytes("family"), //
      Bytes.toBytes("column1"), //
      Bytes.toBytes("value1") //
    )
    put.addColumn(
      Bytes.toBytes("family"), //
      Bytes.toBytes("column2"), //
      Bytes.toBytes("value2") //
    )
    println(put)
    put.getRow.foreach(item => println(item))
  }
}
