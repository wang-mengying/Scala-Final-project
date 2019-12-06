import bean.{Accident, Casualty}
import data.DataSetCreation
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Test {
  def main(args: Array[String]): Unit = {
//    System.setProperty("hadoop.home.dir", "C:\\Program Files\\Hadoop");
//    val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
//    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
//
//    val vehicleDS = DataSetCreation.getVehicleData("in/Vehicles.csv",spark)
//    val accidentDS = DataSetCreation.getAccidentData("in/Accidents.csv",spark)
//    val casualtyDS = DataSetCreation.getCasualtyData("in/Casualties.csv",spark)
//
//    val df: DataFrame = vehicleDS.join(accidentDS, "accident_Index").join(casualtyDS, "accident_Index")
//    df.createTempView("joined")
//    val sql = "select accident_Index,location_Easting_OSGR,vehicle_Reference from joined"
//    spark.sql(sql).show(10)
//    spark.sql("select * from joined").show(1)
val str = "Accident_Index,Location_Easting_OSGR,Location_Northing_OSGR,Longitude,Latitude,Police_Force,Accident_Severity,Number_of_Vehicles,Number_of_Casualties,Date,Day_of_Week,Time,Local_Authority_(District),Local_Authority_(Highway),1st_Road_Class,1st_Road_Number,Road_Type,Speed_limit,Junction_Detail,Junction_Control,2nd_Road_Class,2nd_Road_Number,Pedestrian_Crossing-Human_Control,Pedestrian_Crossing-Physical_Facilities,Light_Conditions,Weather_Conditions,Road_Surface_Conditions,Special_Conditions_at_Site,Carriageway_Hazards,Urban_or_Rural_Area,Did_Police_Officer_Attend_Scene_of_Accident,LSOA_of_Accident_Location"
    str.split(",").map(string => string.substring(0,1).toLowerCase()+string.substring(1)).foreach(println)
  }
}
