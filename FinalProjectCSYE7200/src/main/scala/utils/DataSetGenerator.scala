package utils

import bean.{Accident, Casualty, Vehicle}
import data.DataSetCreation
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

object DataSetGenerator {
  val spark = SparkSessionFactory.getSparkSession
  var vehicleDS:Dataset[Vehicle] = null
  var accidentDS:Dataset[Accident] = null
  var casualtyDS:Dataset[Casualty] = null
  var joinedTable: DataFrame = null

  def init={
    this.vehicleDS = DataSetCreation.getVehicleData("in/Vehicles.csv",spark)
    this.accidentDS = DataSetCreation.getAccidentData("in/Accidents.csv",spark)
    this.casualtyDS = DataSetCreation.getCasualtyData("in/Casualties.csv",spark)
    accidentDS.createGlobalTempView("accident")
    val sql = "select accident_index, accident_severity from accident"
    val accident_serverity: DataFrame = spark.sql(sql)
    this.joinedTable = accident_serverity.join(vehicleDS,"accident_index").join(casualtyDS,"accident_index")
    accidentDS.persist(StorageLevel.MEMORY_AND_DISK_SER)
  }

  def trainSetJoinedGen={
    val trainSetJoined: Dataset[Row] = joinedTable.sample(false,0.75,1L)
    trainSetJoined.persist(StorageLevel.MEMORY_AND_DISK_SER)
    trainSetJoined
  }

  def testSetJoinedGen={
    val testSetJoined: Dataset[Row] = joinedTable.sample(false,0.25,1L)
    testSetJoined.persist(StorageLevel.MEMORY_AND_DISK_SER)
    testSetJoined
  }

  def testSetAccidentGen ={
    val testSetAccident = accidentDS.sample(false,0.25,1L)
    testSetAccident.persist(StorageLevel.MEMORY_AND_DISK_SER)
    testSetAccident
  }

  def trainSetAccidentGen={
    val trainSetAccident = accidentDS.sample(false,0.75,1L)
    trainSetAccident.persist(StorageLevel.MEMORY_AND_DISK_SER)
    trainSetAccident
  }
}
