package app

import bean.{Accident, Casualty, Vehicle}
import data.DataSetCreation
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object App {
  def main(args: Array[String]): Unit = {

    // Zookeeper host:port, group, kafka topic, threads
    if (args.length != 4) {
      println("Usage: StatStreamingApp <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }
    // spark sql initialization
    val conf: SparkConf = new SparkConf().setAppName("CSYE7200FinalProject")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    // Data from local system
    val vehicleDS:Dataset[Vehicle] = DataSetCreation.getVehicleData("in/Vehicles.csv",spark)
    val accidentDS:Dataset[Accident] = DataSetCreation.getAccidentData("in/Accidents.csv",spark)
    val casualtyDS:Dataset[Casualty] = DataSetCreation.getCasualtyData("in/Casualties.csv",spark)

    // streaming preparation
    val Array(zkQuorum, groupId, topics, numThreads) = args
    val streamingContext = new StreamingContext(conf,Seconds(30))
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val messages = KafkaUtils.createStream(streamingContext, zkQuorum, groupId, topicMap)
    // etl
    messages.map(_._2).map {
      d => {
        val data: Array[String] = d.split(",")
        d.split(",").length match {
          case 16 => {
            val ds: Dataset[Casualty] = Seq(Casualty(data(0), data(1), data(2), data(3), data(4), data(5), data(6), data(7), data(8), data(9), data(10), data(11), data(12), data(13), data(14), data(15))).toDS()
            casualtyDS.union(ds)
          }
          case 23 => {
            val ds: Dataset[Vehicle] = Seq(Vehicle(data(0), data(1), data(2), data(3), data(4), data(5), data(6), data(7), data(8), data(9), data(10), data(11), data(12), data(13), data(14), data(15), data(16), data(17), data(18), data(19), data(20), data(21), data(22))).toDS()
            vehicleDS.union(ds)
          }
          case 31 => {
            val ds: Dataset[Accident] = Seq(Accident(data(0), data(1), data(2), data(3), data(4), data(5), data(6), data(7), data(8), data(9), data(10), data(11), data(12), data(13), data(14), data(15), data(16), data(17), data(18), data(19), data(20), data(21), data(22), data(23), data(24), data(25), data(26), data(27), data(28), data(29), data(30))).toDS()
            accidentDS.union(ds)
          }
          case _ => println("Please check your data")
        }
      }
    }

    // machine learning algo
    machineLearning

    streamingContext.start()
    streamingContext.awaitTermination()
  }
  def machineLearning = ???
}
