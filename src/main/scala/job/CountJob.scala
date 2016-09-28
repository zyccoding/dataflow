package job

import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}

/**
  * 消息统计
  * Created by rzx on 2016/9/20.
  */
class CountJob {

  def run(OutputPath: String,
          errorTopic: String,
          messagesDStream: InputDStream[(String, String)],
          brokerList: String,
          separator: String,
          arraySubscript: Int,
          mdbFileName: String,
          messageSize: Int): Unit = {

    var offsetRanges = Array[OffsetRange]()
    val countDStream = messagesDStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.foreachRDD { x =>
      for (o <- offsetRanges) {
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}" + " ------zyc")
        //此处每个分区对应的offset信息封装成一个SQL插入到数据库中
      }

    }

  }


}
