package util

import java.io.{File, FileInputStream}
import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer

/**
  * Created by rzx on 2016/9/18.
  */
object Utils {
  /**
    * 获取自定义配置文件信息
    * @param filePath
    * @return
    */
  def getProp(filePath: String): Properties ={
    val prop = new Properties()
    val in = this.getClass.getResourceAsStream(filePath)
    prop.load(in)
    prop
  }

  /**
    * 获取配置文件信息
    * @return
    */
  def getProp(): Properties ={
    val prop = new Properties()
//    val  fileStr = Thread.currentThread().getContextClassLoader()
//      .getResource("").toURI().getPath();
//    val  file = fileStr+ "prop.properties"  C:\Users\rzx\IdeaProjects\kafka-sparkstreaming\src\main\resources\prop.properties
//    val in = new FileInputStream(new File(file))
//    val in = new FileInputStream(this.getClass.getResource("prop.properties").getFile)
    val in = this.getClass.getResourceAsStream("/prop.properties")
    prop.load(in)
    prop
  }


  /**
    * 实例化KafkaProducer
    * @param brokers
    * @param ack
    * @return
    */
  def getKafkaProducer(brokers: String, ack: String): KafkaProducer[String, String] ={
    val props: Properties = new Properties
    props.put("bootstrap.servers", brokers)
    props.put("acks", ack)
    props.put("retries", "0")
    props.put("linger.ms", "1") //Producer默认会把两次发送时间间隔内收集到的所有Requests进行一次聚合然后再发送
    props.put("buffer.memory", "33554432") //默认是33554432，此配置项是为了解决producer端的消息堆积问题
    props.put("batch.size", "16348") //每个partition对应一个batch.size，此配置项是为了解决producer批量发送
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
    producer
  }


  /**
    * 获取Mysql连接
    * @param property
    * @return
    */
  def getMysqlConnection(property: Properties): Connection = {
    val jdbcUrl = property.getProperty("url") + ";DatabaseName=" + property.getProperty("database")
    val driver = "com.mysql.jdbc.Driver";
    Class.forName(driver).newInstance();
    val conn: Connection = DriverManager.getConnection(jdbcUrl, property.getProperty("username"), property.getProperty("password"))
    conn.setAutoCommit(false)
    conn
  }
}
