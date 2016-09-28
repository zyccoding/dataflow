import java.io.File
import java.net.InetAddress

import com.maxmind.db.CHMCache
import com.maxmind.geoip2.DatabaseReader
import com.maxmind.geoip2.model.CityResponse
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by rzx on 2016/9/5.
  */
object Test {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("zww0902test").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(10))

    val topics = Set("zwwtest", "zwwtest1")
    val brokers = "10.0.1.2:9092,10.0.1.3:9092,10.0.1.4:9092"
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder")


    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    val urlClickLogPairsDStream = kafkaStream.map(_._2)

    val popularityData = urlClickLogPairsDStream.map { msgLine => {

      val dataArr: Array[String] = msgLine.split("\\|")
      val pageID = dataArr(0)
      //calculate the popularity value
      //      val popValue: Double = dataArr(1).toFloat * 0.8 + dataArr(2).toFloat * 0.8 + dataArr(3).toFloat * 1
      //      (pageID, popValue)
      (pageID, msgLine)


    }
    }
    popularityData.foreachRDD(rdd => {
       rdd.mapPartitions(iterator => {
          val url2 = "D:/test/GeoLite2-City.mmdb"
//          val url2 = "/home/soft/GeoLite2-City.mmdb"
          val geoDB = new File(url2)
          val geoIPResolver = new DatabaseReader.Builder(geoDB).withCache(new CHMCache()).build()

          def resolve_ip(resp: CityResponse): (String) = {
            //                 (resp.getCountry.getNames.get("zh-CN").concat("|").concat(resp.getSubdivisions.get(0).getNames.get("zh-CN")).concat("|").concat(resp.getCity.getNames.get("zh-CN")))
            (resp.getCountry.getNames.get("zh-CN"), resp.getSubdivisions.get(0).getNames.get("zh-CN"), resp.getCity.getNames.get("zh-CN"))._1.concat("|") + (resp.getCountry.getNames.get("zh-CN"), resp.getSubdivisions.get(0).getNames.get("zh-CN"), resp.getCity.getNames.get("zh-CN"))._2.concat("|") +
              (resp.getCountry.getNames.get("zh-CN"), resp.getSubdivisions.get(0).getNames.get("zh-CN"), resp.getCity.getNames.get("zh-CN"))._3
          }

         val res = List[String]()
          iterator.foreach(x => {
            if (x != null) {
              try {
                val inetAddress = InetAddress.getByName(x._1)
                val geoResponse = geoIPResolver.city(inetAddress)
                val result = x._2 + "|" + resolve_ip(geoResponse)
                res.::(result)
              } catch {
                case e: Throwable => println("Error in job scheduler", e)
              }

            }
          })
          res.iterator
        })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
