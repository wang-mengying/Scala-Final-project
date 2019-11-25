package dao

/**
 * HBase data storage
 * Prepare for the e-chart data collection and streaming data
 */

import bean.AccidentCount
import org.apache.hadoop.hbase.client.{Get, HTable}
import org.apache.hadoop.hbase.util.Bytes
import utils.HBaseUtils

import scala.collection.mutable.ListBuffer

class AccidentDAO {
  val tableName = "accident"
  val cf = "info"
  val qualifier = "total"

  def save(list:ListBuffer[AccidentCount])={
    val table = HBaseUtils.getInstance().getTable(tableName)
    for (elem <- list) {
      table.incrementColumnValue(Bytes.toBytes(elem.roadType_speedLimit),Bytes.toBytes(cf),Bytes.toBytes(qualifier),elem.count)
    }
  }

  def count(roadType_speedLimit:String)={
    val table: HTable = HBaseUtils.getInstance().getTable(tableName)
    val get = new Get(Bytes.toBytes(roadType_speedLimit))
    val value = table.get(get).getValue(cf.getBytes(),qualifier.getBytes())
    if(value == null) 0L
    else Bytes.toLong(value)
  }

}
