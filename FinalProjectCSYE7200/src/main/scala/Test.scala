import bean.Accident
import data.DataSetCreation
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Test {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val vehicleDS = DataSetCreation.getVehicleData("in/Vehicles.csv",spark)
    val accidentDS = DataSetCreation.getAccidentData("in/Accidents.csv",spark)
    val casualtyDS = DataSetCreation.getCasualtyData("in/Casualties.csv",spark)


    vehicleDS.show(5)
    accidentDS.show(5)
    casualtyDS.show(5)

  }
}
