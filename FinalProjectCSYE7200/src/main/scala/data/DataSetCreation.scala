package data

import bean.{Accident, Casualty, Vehicle}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

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
        DataCleaning.parseVehicle(data)
      }
    }.toDS()
    ds
  }
  def getCasualtyData(path:String,spark:SparkSession)={
    import spark.implicits._
    val rdd = spark.sparkContext.textFile(path)
    val ds: Dataset[Casualty] = rdd.map(d => {
      val data = d.split(",")
      DataCleaning.parseCasualty(data)
    }).toDS()
    ds
  }
  def getAccidentData(path:String,spark:SparkSession)={
    import spark.implicits._
    val rdd = spark.sparkContext.textFile(path)
    val ds: Dataset[Accident] = rdd.map(d => {
      val data = d.split(",")
      DataCleaning.parseAccident(data)
    }).toDS()
    ds
  }
}