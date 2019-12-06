package utils
import java.io
import bean.{Accident, Casualty, Vehicle}
import org.apache.spark.streaming.dstream.DStream

object StreamingDataManipulation {

  def transform(input: DStream[io.Serializable])={
    input.foreachRDD(
      strs => {
        val data: Array[io.Serializable] = strs.collect()
        data.foreach(d => d match {
          case casualty: Casualty => println(casualty)
          case accident: Accident => println(accident)
          case vehicle: Vehicle => println(vehicle)
          case _ => println("Data Parsing Failed")
        })}
    )
  }
}
