package util

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

/**
  * Created by rzx on 2016/9/27.
  */
object BroadCastSource {
  @volatile private var instance: Broadcast[Seq[String]] = null
  def getInstance(sc: SparkContext): Broadcast[Seq[String]] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          val wordBlacklist = Seq("a", "b", "c")
          instance = sc.broadcast(wordBlacklist)
        }
      }
    }
    instance
  }

}
