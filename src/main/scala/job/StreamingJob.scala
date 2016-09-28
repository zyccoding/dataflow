package job

import java.io.File
import java.net.InetAddress

import com.maxmind.db.CHMCache
import com.maxmind.geoip2.DatabaseReader
import com.maxmind.geoip2.model.CityResponse
import org.apache.kafka.clients.producer.{Producer, ProducerRecord}
import org.apache.spark.SparkFiles
import org.apache.spark.streaming.dstream.InputDStream
import util.Utils

import scala.collection.mutable.ListBuffer
/**
  * Created by rzx on 2016/9/19.
  */
class StreamingJob {

  def run(OutputPath: String,
          errorTopic: String,
          messagesDStream: InputDStream[(String, String)],
          brokerList: String,
          separator: String,
          arraySubscript: Int,
          mdbFileName: String,
          messageSize: Int): Unit = {
    //初始化DStream
    messagesDStream.foreachRDD(x => {
      x.mapPartitions(rdd => {
        //init kafka producer
        val producer: Producer[String, String] = Utils.getKafkaProducer(brokerList, "all")

        //geoIp2
        val file = new File(SparkFiles.get(mdbFileName))

        val geoIPResolver = new DatabaseReader.Builder(file).withCache(new CHMCache()).build()
        def resolve_ip(resp: CityResponse): (String) = {
          (resp.getCountry.getNames.get("zh-CN"), resp.getSubdivisions.get(0).getNames.get("zh-CN"), resp.getCity.getNames.get("zh-CN"))._1.concat("|") + (resp.getCountry.getNames.get("zh-CN"), resp.getSubdivisions.get(0).getNames.get("zh-CN"), resp.getCity.getNames.get("zh-CN"))._2.concat("|") +
            (resp.getCountry.getNames.get("zh-CN"), resp.getSubdivisions.get(0).getNames.get("zh-CN"), resp.getCity.getNames.get("zh-CN"))._3
        }
        val list = ListBuffer[String]()
        rdd.foreach(line => {
          val value = line._2.split(separator)
          //判断长度
          if(value.length != messageSize){
            producer.send(new ProducerRecord[String, String](errorTopic, line._1, line._2))
          } else {
            val ip = line._2.split(separator)(arraySubscript)
            try {
              val intAddress = InetAddress.getByName(ip)
              val geoResponse = geoIPResolver.city(intAddress)
              val result = "key = " + line._1 + " _____ "  + "|" + resolve_ip(geoResponse)
              list.append(result)
            } catch {
              case ex: Exception => producer.send(new ProducerRecord[String, String](errorTopic, line._1, line._2))
            }
          }

        })
        list.toIterator
      })
//        .saveAsTextFile(OutputPath + System.currentTimeMillis().toString)

    })

  }
}
