import bean.{Accident, Casualty}
import data.DataSetCreation
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Test {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Program Files\\Hadoop");
    val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val vehicleDS = DataSetCreation.getVehicleData("in/Vehicles.csv",spark)
//    val accidentDS = DataSetCreation.getAccidentData("in/Accidents.csv",spark)
//    val casualtyDS = DataSetCreation.getCasualtyData("in/Casualties.csv",spark)
//
//    val df: DataFrame = vehicleDS.join(accidentDS, "accident_Index").join(casualtyDS, "accident_Index")
//    df.createTempView("joined")
//    val sql = "select accident_Index,location_Easting_OSGR,vehicle_Reference from joined"
//    spark.sql(sql).show(10)
//    spark.sql("select * from joined").show(1)
//    val str = "Accident_Index,Vehicle_Reference,Vehicle_Type,Towing_and_Articulation,Vehicle_Manoeuvre,Vehicle_Location-Restricted_Lane,Junction_Location,Skidding_and_Overturning,Hit_Object_in_Carriageway,Vehicle_Leaving_Carriageway,Hit_Object_off_Carriageway,1st_Point_of_Impact,Was_Vehicle_Left_Hand_Drive?,Journey_Purpose_of_Driver,Sex_of_Driver,Age_of_Driver,Age_Band_of_Driver,Engine_Capacity_(CC),Propulsion_Code,Age_of_Vehicle,Driver_IMD_Decile,Driver_Home_Area_Type,Vehicle_IMD_Decile"
//    str.split(",").map(_.replaceAll("-","_").replaceAll("\\(","").replaceAll("\\)","").toLowerCase).foreach(println)
    vehicleDS.createOrReplaceTempView("StudentTable")
    spark.sql("select * from StudentTable").show()
  }
}
