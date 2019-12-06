package data

import bean.{Accident, Casualty, Vehicle}

object DataCleaning {
  def parseData(str:String):Serializable={
    val data: Array[String] = str.split(",")
    data.length match {
      case 16 => parseCasualty(data)
      case 23 => parseVehicle(data)
      case 32 => parseAccident(data)
    }
  }

  def parseCasualty(data:Array[String]):Casualty ={
    val res = Casualty(data(0), data(1), data(2), data(3), data(4), data(5).toInt, data(6), data(7).toInt, data(8), data(9), data(10), data(11), data(12), data(13), data(14), data(15).toInt)
    res
  }

  def parseAccident(data:Array[String]):Accident={
    val res = Accident(data(0), data(1), data(2), data(3), data(4), data(5), data(6).toInt, data(7).toInt, data(8).toInt, data(9), data(10), data(11), data(12), data(13), data(14), data(15), data(16), data(17).toInt, data(18), data(19), data(20), data(21), data(22), data(23), data(24), data(25), data(26), data(27), data(28), data(29), data(30))
    res
  }

  def parseVehicle(data:Array[String]):Vehicle={
    val res: Vehicle = Vehicle(data(0), data(1), data(2), data(3), data(4), data(5), data(6), data(7), data(8), data(9), data(10), data(11), data(12), data(13), data(14), data(15).toInt, data(16), data(17).toInt, data(18), data(19).toInt, data(20).toInt, data(21), data(22))
    res
  }
}