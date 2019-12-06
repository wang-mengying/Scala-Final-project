package utils

import bean.{Accident, Casualty, Vehicle}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkSessionFactory {

  var spark:SparkSession = null

  def getSparkSession:SparkSession = {
    if(spark == null){
      val conf: SparkConf = new SparkConf().setAppName("CSYE7200FinalProject").set("spark.storage.memoryFraction","0.6").set("spark.shuffle.file.buffer","64")
        .set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" ).setMaster("local[*]")
      spark = SparkSession.builder().config(conf).getOrCreate()
      conf.registerKryoClasses(Array(classOf[Accident],classOf[Casualty],classOf[Vehicle],classOf[scala.collection.mutable.WrappedArray.ofRef[_]]))
    }
    spark
  }

  def getStreamingContext:StreamingContext={
    val streamingContext = new StreamingContext(spark.sparkContext,Seconds(45))
    streamingContext
  }

}
