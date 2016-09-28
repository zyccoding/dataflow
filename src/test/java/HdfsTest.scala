import org.apache.hadoop.conf.Configuration
import java.io.{File, FileInputStream, IOException, InputStream}

import com.maxmind.db.CHMCache
import com.maxmind.geoip2.DatabaseReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
import org.apache.spark.{SparkConf, SparkContext, SparkFiles};
/**
  * Created by rzx on 2016/9/8.
  */
object HdfsTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Kafka to HDFS").setMaster("yarn-cluster")
    val sc = new SparkContext(sparkConf)
    sc.addFile("hdfs:///user/root/file/GeoLite2-City.mmdb") //采用 SparkContext.assFile()形式


    val file = new File(SparkFiles.get("GeoLite2-City.mmdb"))

  }
}
