package data

import bean.{Accident, Casualty, Vehicle}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * Read Data from file system
 */

object DataSetCreation {
  def getVehicleData(path:String,spark:SparkSession)= {
    import spark.implicits._
    val rdd: RDD[String] = spark.sparkContext.textFile(path)
    val ds: Dataset[Vehicle] = rdd.map {
      d => {
        val data: Array[String] = d.split(",")
        Vehicle(data(0), data(1), data(2), data(3), data(4), data(5), data(6), data(7), data(8), data(9), data(10), data(11), data(12), data(13), data(14), data(15), data(16), data(17), data(18), data(19), data(20), data(21), data(22))
      }
    }.toDS()
    ds
  }
  def getCasualtyData(path:String,spark:SparkSession)={
    import spark.implicits._
    val rdd = spark.sparkContext.textFile(path)
    val ds: Dataset[Casualty] = rdd.map(d => {
      val data = d.split(",")
      Casualty(data(0), data(1), data(2), data(3), data(4), data(5), data(6), data(7), data(8), data(9), data(10), data(11), data(12), data(13), data(14), data(15))
    }).toDS()
    ds
  }
  def getAccidentData(path:String,spark:SparkSession)={
    import spark.implicits._
    val rdd = spark.sparkContext.textFile(path)
    val ds: Dataset[Accident] = rdd.map(d => {
      val data = d.split(",")
      Accident(data(0),data(1),data(2),data(3),data(4),data(5),data(6),data(7),data(8),data(9),data(10),data(11),data(12),data(13),data(14),data(15),data(16),data(17),data(18),data(19),data(20),data(21),data(22),data(23),data(24),data(25),data(26),data(27),data(28),data(29),data(30))
    }).toDS()
    ds
  }
}