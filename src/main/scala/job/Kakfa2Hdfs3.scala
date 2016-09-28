package job

import java.io.File
import java.net.InetAddress

import com.maxmind.db.CHMCache
import com.maxmind.geoip2.DatabaseReader
import com.maxmind.geoip2.model.CityResponse
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.streaming.kafka.{KafkaUtils, OffsetRange}

import scala.collection.mutable.ListBuffer

/**
  * Created by rzx on 2016/9/13.
  */
object Kakfa2Hdfs3 {

  def main(args: Array[String]): Unit = {
    val topicArray = args(0)
    val Array(brokers, topics) = Array("10.0.1.4:2181", topicArray)

    //初始化SSC
    val sparkConf = new SparkConf().setAppName("Kafka to HDFS").setMaster("yarn-cluster")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    val sparkStreamContext = new StreamingContext(sparkConf,Seconds(args(1).toInt))
    val sparkContext = new SparkContext(sparkConf)
    //初始化配置
    sparkContext.addFile("hdfs:///user/root/file/GeoLite2-City.mmdb") //采用 SparkContext.assFile()形式
    val topicSet  = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "10.0.1.3:9092,10.0.1.2:9092", "auto.offset.reset" -> "smallest")



    //初始化DStream
    val offsetRange = Array(OffsetRange("Bot_or_Trojan_B_IP",0,6932826,8041704),
      OffsetRange(topicArray, 1, 0, 8041704),
      OffsetRange(topicArray, 2, 0, 8041704),
      OffsetRange(topicArray, 3, 0, 8041704),
      OffsetRange(topicArray, 4, 0, 8041704),
      OffsetRange(topicArray, 5, 0, 8041704),
      OffsetRange(topicArray, 6, 0, 8041704),
      OffsetRange(topicArray, 7, 0, 8041704),
      OffsetRange(topicArray, 8, 0, 8041704),
      OffsetRange(topicArray, 9, 0, 8041704),
      OffsetRange(topicArray, 10, 0, 8041704),
      OffsetRange(topicArray, 11, 0, 8041704))

    //获取每个partition的offset，循环构建OffsetRange


    KafkaUtils.createRDD[String,String,StringDecoder,StringDecoder](sparkContext,kafkaParams,offsetRange)
      .mapPartitions(rdd => {
        val file = new File(SparkFiles.get("GeoLite2-City.mmdb"))
        val geoIPResolver = new DatabaseReader.Builder(file).withCache(new CHMCache()).build()
        def resolve_ip(resp: CityResponse): (String) = {
          (resp.getCountry.getNames.get("zh-CN"), resp.getSubdivisions.get(0).getNames.get("zh-CN"), resp.getCity.getNames.get("zh-CN"))._1.concat("|") + (resp.getCountry.getNames.get("zh-CN"), resp.getSubdivisions.get(0).getNames.get("zh-CN"), resp.getCity.getNames.get("zh-CN"))._2.concat("|") +
            (resp.getCountry.getNames.get("zh-CN"), resp.getSubdivisions.get(0).getNames.get("zh-CN"), resp.getCity.getNames.get("zh-CN"))._3
        }
        def catchex(): Unit ={

        }
        val list = ListBuffer[String]()
        rdd.foreach(line => {

          try{
            val ip = line._2.split("<--->")(10)
            val intAddress = InetAddress.getByName(ip)
            val geoResponse = geoIPResolver.city(intAddress)
            val result = line._2 + "|" + resolve_ip(geoResponse)
            list.append(result)
          }catch {
            case ex: Exception =>
          }
        })
        list.toIterator
      }).saveAsTextFile("/user/root/zyc/" + System.currentTimeMillis().toString)


    sparkContext.stop()
  }

}
