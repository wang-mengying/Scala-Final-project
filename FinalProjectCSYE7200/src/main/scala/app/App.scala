package app
import bean.{Accident, Casualty, Vehicle}
import data.DataCleaning
import org.apache.spark.sql.{Dataset, _}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.DataSetGenerator.spark
import utils.{DataSetGenerator, SparkSessionFactory}

object App {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Program Files\\Hadoop");
// Zookeeper host:port, group, kafka topic, threads
//    if (args.length != 4) {
//      println("Usage: StatStreamingApp <zkQuorum> <group> <topics> <numThreads>")
//      System.exit(1)
//    }
// spark sql initialization
// streaming preparation
// val Array(zkQuorum, groupId, topics, numThreads) = args
// val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
// val messages = KafkaUtils.createStream(streamingContext, zkQuorum, groupId, topicMap)
// Streaming Data
    val spark = SparkSessionFactory.getSparkSession
    import spark.implicits._
    val streamingContext = new StreamingContext(spark.sparkContext,Seconds(5))
    DataSetGenerator.init
    var accidentDS: Dataset[Accident] = DataSetGenerator.accidentDS
    var vehicleDS: Dataset[Vehicle] = DataSetGenerator.vehicleDS
    var casualtyDS = DataSetGenerator.casualtyDS
    var joinedTable_train: Dataset[Row] = DataSetGenerator.trainSetJoinedGen
    var joinedTable_test: Dataset[Row] = DataSetGenerator.testSetJoinedGen
    var accident_train: Dataset[Accident] = DataSetGenerator.trainSetAccidentGen
    var accident_test: Dataset[Accident] = DataSetGenerator.testSetAccidentGen

    val streamingData: ReceiverInputDStream[String] = streamingContext.socketTextStream("hadoop000", 9999)
    val input: DStream[Serializable] = streamingData.map (data => DataCleaning.parseData(data))
    input.foreachRDD(
      strs => {
        val data: Array[Serializable] = strs.collect()
        data.foreach(d => d match {
          case casualty: Casualty => {
            casualtyDS = casualtyDS.union(Seq(casualty).toDS())
          }
          case accident: Accident => {
            accidentDS = accidentDS.union(Seq(accident).toDS())
          }
          case vehicle: Vehicle => {
            vehicleDS = vehicleDS.union(Seq(vehicle).toDS())
            accidentDS.createOrReplaceTempView("accident")
            val sql = "select accident_index, accident_severity from accident"
            val accident_serverity: DataFrame = spark.sql(sql)
            joinedTable_train = accident_serverity.join(vehicleDS,"accident_index").join(casualtyDS,"accident_index")
            accident_serverity.orderBy("accident_index").show(1)
            println("Data Added")
          }
          case _ => println("Data Parsing Failed")
        })}
    )
    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def linearregressor = {
    val df_casualty_train: DataFrame = spark.sql("select ‘accident_severity’, ‘number_of_vehicles’, ‘number_of_casualties’, ‘date’, ‘day_of_week’, ‘time’, ‘road_type’, ‘speed_limit’, ‘junction_detail’, ‘junction_control’, ‘pedestrian_crossing_human_control’, ‘pedestrian_crossing_physical_facilities’, ‘light_conditions’, ‘weather_conditions’, ‘special_conditions_at_site’, ‘carriageway_hazards’, ‘urban_or_rural_area’, ‘did_police_officer_attend_scene_of_accident’ from joinedTable_train").withColumnRenamed("casualty_severity", "label")
    val df_casualty_test: DataFrame = spark.sql("select ‘accident_severity’, ‘number_of_vehicles’, ‘number_of_casualties’, ‘date’, ‘day_of_week’, ‘time’, ‘road_type’, ‘speed_limit’, ‘junction_detail’, ‘junction_control’, ‘pedestrian_crossing_human_control’, ‘pedestrian_crossing_physical_facilities’, ‘light_conditions’, ‘weather_conditions’, ‘special_conditions_at_site’, ‘carriageway_hazards’, ‘urban_or_rural_area’, ‘did_police_officer_attend_scene_of_accident’ from joinedTable_train").withColumnRenamed("casualty_severity", "label")
    df_casualty_train.show(5)
  }





