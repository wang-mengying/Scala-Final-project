package app

import org.apache.spark.sql.SparkSession

object LinearRegression {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Accident_lr")
      .master("local[*]")
      .getOrCreate()

    val df = spark
      .read
      .option("header","true")
      .option("inferschema","true")
      .csv("FinalProjectCSYE7200/src/main/scala/app/data/Accidents.csv")

    df.show(5)
  }
}
