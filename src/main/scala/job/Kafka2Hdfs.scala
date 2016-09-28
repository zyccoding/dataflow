package job

import java.io.File
import java.net.InetAddress

import com.maxmind.db.CHMCache
import com.maxmind.geoip2.DatabaseReader
import com.maxmind.geoip2.model.CityResponse
import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkFiles}
import org.apache.spark.streaming.kafka.{KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.Properties
import scala.collection.mutable.ListBuffer

/**
  * Created by rzx on 2016/9/6.
  */
object Kafka2Hdfs {

  def main(args: Array[String]): Unit = {

    val Array(brokers, topics) = Array("10.0.1.4:2181",args(0))

    //初始化SSC
    val sparkConf = new SparkConf().setAppName("Kafka to HDFS").setMaster("yarn-cluster")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    sparkConf.set("spark.executor.userClassPathFirst","true")
//    sparkConf.set("spark.driver.userClassPathFirst","true")
    val sparkStreamContext = new StreamingContext(sparkConf,Seconds(args(1).toInt))

    //初始化配置
    sparkStreamContext.sparkContext.addFile("hdfs:///user/root/file/GeoLite2-City.mmdb") //采用 SparkContext.assFile()形式
    val topicSet  = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "10.0.1.2:9092,10.0.1.3:9092,10.0.1.4:9092", "auto.offset.reset" -> "smallest")


    //初始化DStream
    val messagesDStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](sparkStreamContext,kafkaParams,topicSet)

    messagesDStream.foreachRDD(x => {
      x.mapPartitions(rdd => {
        //init kafka producer
        val props: Properties = new Properties
        props.put("bootstrap.servers", "10.0.1.2:9092,10.0.1.3:9092")
        props.put("acks", "all")
        props.put("retries", "0")
        props.put("linger.ms", "1") //Producer默认会把两次发送时间间隔内收集到的所有Requests进行一次聚合然后再发送
        props.put("buffer.memory", "33554432") //默认是33554432，此配置项是为了解决producer端的消息堆积问题
        props.put("batch.size", "16348") //每个partition对应一个batch.size，此配置项是为了解决producer批量发送
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        val producer: Producer[String, String] = new KafkaProducer[String, String](props)

        //geoip2
        val file = new File(SparkFiles.get("GeoLite2-City.mmdb"))
        val geoIPResolver = new DatabaseReader.Builder(file).withCache(new CHMCache()).build()
        def resolve_ip(resp: CityResponse): (String) = {
          (resp.getCountry.getNames.get("zh-CN"), resp.getSubdivisions.get(0).getNames.get("zh-CN"), resp.getCity.getNames.get("zh-CN"))._1.concat("|") + (resp.getCountry.getNames.get("zh-CN"), resp.getSubdivisions.get(0).getNames.get("zh-CN"), resp.getCity.getNames.get("zh-CN"))._2.concat("|") +
            (resp.getCountry.getNames.get("zh-CN"), resp.getSubdivisions.get(0).getNames.get("zh-CN"), resp.getCity.getNames.get("zh-CN"))._3
        }
        val list = ListBuffer[String]()
        rdd.foreach(line => {
//          val ip = line._2.split("<--->")(10)
          val ip = line._2
          try{
            val intAddress = InetAddress.getByName(ip)
            val geoResponse = geoIPResolver.city(intAddress)
            val result = line._2 + "|" + resolve_ip(geoResponse)
            list.append(result)
          }catch {
            case ex: Exception => producer.send(new ProducerRecord[String, String]("errormes", line._1, line._2))
          }
        })
        list.toIterator
      }).saveAsTextFile("/user/root/zyc/" + System.currentTimeMillis().toString)

    })

    sparkStreamContext.start()
    sparkStreamContext.awaitTermination()

  }

  def send(topic: String, key: String, value: String): Unit ={

  }


}
