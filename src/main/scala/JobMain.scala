import java.text.SimpleDateFormat

import job.Kafka2Hdfs2
import kafka.serializer.StringDecoder
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.{Enum, Utils}

/**
  * 主程序入口
  * Created by rzx on 2016/9/18.
  */
object JobMain {
    val prop = Utils.getProp()
    val brokerList = prop.getProperty("brokerList")
    val mdbPath = prop.getProperty("GeoLite2-City.mmdb.path")
    val OutputPath = prop.getProperty("outputPath")
    val errorTopic = prop.getProperty("errorTopic")
    val batch =  prop.getProperty("batch").toInt
    val topics = prop.getProperty("topics")
    val separator = prop.getProperty("separator")
    val arraySubscript = prop.getProperty("arraySubscript")
    val mdbFileName = prop.getProperty("mdbFileName")
    val messageSize = prop.getProperty("messageSize").toInt
    val checkpointDirectory = prop.getProperty("checkpointDirectory")
    val sdf = new SimpleDateFormat("yyyy/MM/dd")

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: ")
      System.exit(1)
    }
    //    val sparkStreamContext = new StreamingContext(sparkConf,Seconds(batch))
    val sparkStreamContext = StreamingContext.getOrCreate(checkpointDirectory, () => createStreamingContext(args(0), args(1), 10))

    val checkpointPath = new Path(checkpointDirectory)
    val fs: FileSystem = checkpointPath.getFileSystem(SparkHadoopUtil.get.conf)
    if (fs.exists(checkpointPath)) {
      sparkStreamContext.sparkContext.addFile(mdbPath) //采用 SparkContext.assFile()形式
    }

    sparkStreamContext.start()
    sparkStreamContext.awaitTermination()
  }

  /**
    * 初始化StreamingContext
    * @param topics
    * @param errorTopic
    * @param second
    * @return
    */
  def createStreamingContext(topics: String, errorTopic: String, second: Int): StreamingContext = {
    val sparkConf = new SparkConf().setAppName("Kafka to HDFS").setMaster("yarn-cluster")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkStreamContext = new StreamingContext(sparkConf, Seconds(second))
    sparkStreamContext.checkpoint(checkpointDirectory)

    //初始化配置
    sparkStreamContext.sparkContext.addFile(mdbPath) //采用 SparkContext.assFile()形式
    val topicSet = topics.split(",").toSet
    println(brokerList + "--zyc")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokerList, "auto.offset.reset" -> "smallest")

    //获取DStream
    val messagesDStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](sparkStreamContext,kafkaParams,topicSet)

    //开始清洗任务
    Kafka2Hdfs2.run(OutputPath, errorTopic, messagesDStream, brokerList,
      separator, arraySubscript.toInt, mdbFileName, messageSize)

    //开始统计任务
    //    val countJob = new CountJob
    //    countJob.run(OutputPath, args(1), messagesDStream, brokerList, separator, arraySubscript.toInt, mdbFileName, messageSize)

    sparkStreamContext
  }

}
