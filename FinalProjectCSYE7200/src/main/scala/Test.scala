import bean.{Accident, Casualty}
import data.DataSetCreation
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Test {
  def main(args: Array[String]): Unit = {
//    val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
//    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
//
//    val vehicleDS = DataSetCreation.getVehicleData("in/Vehicles.csv",spark)
//    val accidentDS = DataSetCreation.getAccidentData("in/Accidents.csv",spark)
//    val casualtyDS = DataSetCreation.getCasualtyData("in/Casualties.csv",spark)
//
//
//    vehicleDS.show(5)
//    accidentDS.show(5)
//    casualtyDS.show(5)
//    val seq: Seq[Int] = Seq(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23)
//    val value: RDD[Int] = spark.sparkContext.makeRDD(seq)
//    import spark.implicits._
//
//    val ds: Dataset[Casualty] = seq.toDF().as[Casualty]
//    ds.show()
  }
}
