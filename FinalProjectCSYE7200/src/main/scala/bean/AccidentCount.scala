package bean

/**
 * class for data storage in HBase
 * @param roadType_speedLimit HBase Row Key
 * @param count
 */
case class AccidentCount(roadType_speedLimit : String, count:Int)
