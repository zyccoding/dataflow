package job

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * 周期报表任务
  * Created by rzx on 2016/9/26.
  */
object PeriodFormJob {


  def run(tableName: String, partitionPath: String): Unit ={
     val conf = new SparkConf().setAppName("").setMaster("")
     val sparkContext = new SparkContext(conf)
     val SqlContext = new SQLContext(sparkContext)
     val hiveContext = new HiveContext(sparkContext)

     import SqlContext.implicits._
     import org.apache.spark.sql.functions._
    import hiveContext.sql

      val table  = sql("select * from dnsreal where hour = ???").cache()


  }

}
