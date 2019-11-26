package app

import bean.{Accident, AccidentCount, Casualty, Vehicle}
import dao.AccidentDAO
import data.DataSetCreation
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.HBaseUtils

object App {
  def main(args: Array[String]): Unit = {

    // Zookeeper host:port, group, kafka topic, threads
    if (args.length != 4) {
      println("Usage: StatStreamingApp <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }
    // spark sql initialization
    val conf: SparkConf = new SparkConf().setAppName("CSYE7200FinalProject").set("spark.storage.memoryFraction","0.6").set("spark.shuffle.file.buffer","64")
      .set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
    conf.registerKryoClasses(Array(classOf[Accident],classOf[Casualty],classOf[Vehicle],classOf[scala.collection.mutable.WrappedArray.ofRef[_]]))
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    // Data from local system
    val vehicleDS:Dataset[Vehicle] = DataSetCreation.getVehicleData("in/Vehicles.csv",spark)
    val accidentDS:Dataset[Accident] = DataSetCreation.getAccidentData("in/Accidents.csv",spark)
    val casualtyDS:Dataset[Casualty] = DataSetCreation.getCasualtyData("in/Casualties.csv",spark)
    vehicleDS.persist(StorageLevel.MEMORY_AND_DISK_SER)
    accidentDS.persist(StorageLevel.MEMORY_AND_DISK_SER)
    casualtyDS.persist(StorageLevel.MEMORY_AND_DISK_SER)
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
            AccidentDAO.save(AccidentCount(data(16)+"_"+data(17),1))
          }
          case _ => println("Please check your data")
        }
      }
    }
//    casualtyDS.show(5)
//    vehicleDS.show(5)
//    accidentDS.show(5)

    // machine learning algo
    machineLearning

    streamingContext.start()
    streamingContext.awaitTermination()
  }
  def machineLearning = ???
}
