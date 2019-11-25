package app

import data.DataSetCreation
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object App {
  def main(args: Array[String]): Unit = {

    // Zookeeper host:port, group, kafka topic, threads
    if (args.length != 4) {
      println("Usage: StatStreamingApp <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }
    val Array(zkQuorum, groupId, topics, numThreads) = args
    val conf: SparkConf = new SparkConf().setAppName("CSYE7200FinalProject")
    // Retrieve data once per minute
    val streamingContext = new StreamingContext(conf,Seconds(60))
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    //get message from kafka
    val messages = KafkaUtils.createStream(streamingContext, zkQuorum, groupId, topicMap)
    val logs = messages.map(_._2)
    val vehicleDS = DataSetCreation.getVehicleData("in/Vehicles.csv",spark)
    val accidentDS = DataSetCreation.getAccidentData("in/Accidents.csv",spark)
    val casualtyDS = DataSetCreation.getCasualtyData("in/Casualties.csv",spark)

    // machine learning algo
    machineLearning


    streamingContext.start()
    streamingContext.awaitTermination()
  }
  def machineLearning = ???
}
