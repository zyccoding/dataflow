package util

import java.text.SimpleDateFormat

import org.apache.spark.sql.hive.HiveContext

/**
  * Created by rzx on 2016/9/19.
  */
object CommonUtils {

  def save2HDFS(path: String): Unit ={

  }
  /**
    *  向Hive表添加分区
    *
    * @param hiveContext   hive上下文
    * @param path          路径
    * @param tableName    表名
    * @param date         日期
    */
  def addHivePartition(hiveContext: HiveContext, path: String, tableName: String, date: java.util.Date): Unit ={
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val sdf2 = new SimpleDateFormat("yyyy/MM/dd/HH")
    val dropPartitionSql = "ALTER TABLE " +  tableName + "  DROP IF EXISTS PARTITION (hour='"+ sdf.format(date) + "')"
    println("dropPartitionSql: " + dropPartitionSql)
    val addPartitionSql = "alter table " + tableName + " add  partition(hour='" + sdf.format(date) + "') location '" + path + "/"  + sdf2.format(date) + "'"
    println("addPartitionsSql: " + addPartitionSql)
    hiveContext.sql(dropPartitionSql)
    hiveContext.sql(addPartitionSql)
  }

}
