package util

/**
  * Created by rzx on 2016/9/22.
  */
object Enum extends Enumeration {
  type Enumeration = Value
  val prop = Utils.getProp()
  val BROKER_LIST = Value(prop.getProperty("brokerList"))
  val MMDB_PATH = Value(prop.getProperty("GeoLite2-City.mmdb.path"))
  val OUTPUT_PATH = Value(prop.getProperty("outputPath"))
  val ERROR_TOPIC = Value(prop.getProperty("errorTopic"))
  val BATCH = Value(prop.getProperty("batch").toInt)
  val TOPICS = Value(prop.getProperty("topics"))
  val SEPARATOR = Value(prop.getProperty("separator"))
  val ARRAY_SUBSCRIPT = Value(prop.getProperty("arraySubscript"))
  val MMDB_FILENAME = Value(prop.getProperty("mdbFileName"))
  val MESSAGE_SIZE = Value(prop.getProperty("messageSize"))
  val CHECKPOINT_DIRECTORY = Value(prop.getProperty("checkpointDirectory"))
}
