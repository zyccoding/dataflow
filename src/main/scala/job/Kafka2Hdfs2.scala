package job

import java.io.File
import java.net.InetAddress
import java.text.SimpleDateFormat
import java.util.Date

import com.maxmind.db.CHMCache
import com.maxmind.geoip2.DatabaseReader
import com.maxmind.geoip2.model.CityResponse
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.kafka.clients.producer.{Producer, ProducerRecord}
import org.apache.spark.SparkFiles
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import util.Utils

import scala.collection.mutable.ListBuffer

/**
  * Created by rzx on 2016/9/12.
  */
object Kafka2Hdfs2 {
  val sdf = new SimpleDateFormat("yyyy/MM/dd/HH")

  /**
    *
    * @param OutputPath      输出路径
    * @param errorTopic      异常TOPIC
    * @param messagesDStream DStream
    * @param brokerList      broker集合
    * @param separator       行分隔符
    * @param arraySubscript  ip下标
    * @param mdbFileName     mdb文件名
    */
  def run(OutputPath: String,
          errorTopic: String,
          messagesDStream: InputDStream[(String, String)],
          brokerList: String,
          separator: String,
          arraySubscript: Int,
          mdbFileName: String,
          messageSize: Int): Unit = {
    var offsetRanges = Array[OffsetRange]()

    //初始化DStream
    messagesDStream
      .transform { rdd =>
        //HasOffsetRanges每个batch中用于获取每个partition的offset
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
      .foreachRDD(rdd => {
//                for(o <- offsetRanges){
//                  println(s"${o.topic}  ${o.partition}  ${o.fromOffset}  ${o.untilOffset}" + " ------zyc")
////                  val fileName = System.currentTimeMillis().toString
////                  val fileName = offsetRanges.head.partition + "---" + System.currentTimeMillis()
//                }
        val fileOutputPath = OutputPath + sdf.format(new Date()) + "/" + System.currentTimeMillis()

        rdd.mapPartitions(partition => {
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

          partition.foreach(line => {
            val value = line._2.split(separator)
            //判断长度
            if (value.length != messageSize) {
              producer.send(new ProducerRecord[String, String](errorTopic, line._1, line._2))
            } else {
              val ip = line._2.split(separator)(arraySubscript)
              try {
                val intAddress = InetAddress.getByName(ip)
                val geoResponse = geoIPResolver.city(intAddress)
                val resultBuilder = new StringBuilder(line._2.replace(separator, "\t"))
                resultBuilder.append("\t")
                resultBuilder.append(resolve_ip(geoResponse))
                val result = resultBuilder.toString
                //              val result = "key = " + line._1 + " _____ "  + "|" + resolve_ip(geoResponse)
                list.append(result)
              } catch {
                case ex: Exception => producer.send(new ProducerRecord[String, String](errorTopic, line._1, line._2))
              }
            }

          })
          list.toIterator
          //          (fileName,list.toIterator)
        }).saveAsTextFile(fileOutputPath)
//          .map(x => {
//              每个分区对应一个唯一Key值
//            (x,x)
//          })
//          .map(x => (fileName, x))
//         .saveAsHadoopFile(OutputPath + sdf.format(new Date()), classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])
        //为了提高输出的并行度，fileName可以定义为以每个分区生成一个文件，比如fileName=offsetRanges.head.partition
        //        .saveAsTextFile(OutputPath + System.currentTimeMillis().toString)
      })

  }

}

class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = key.asInstanceOf[String]

}
