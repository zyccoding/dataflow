package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * Created by rzx on 2016/9/8.
 */
public class KafkaProducerMain implements Runnable{
    private static int size = 1;
    private String topic;
    private static String message = null;
    private static String key = "5";
    private static String value = "CNCERT<--->917MT<--->404004<--->恶意代码<--->木马<--->远程控制木马<--->木马-远控-白金远控-基础2<--->2016-05-16 23:57:35<--->20160516<--->23<--->119.2.4.204<--->3275948090<--->54229<--->219.139.115.215<--->3683349463<--->8086<--->1<--->r=cb1st\\16\\00\\00\\00\\01\\00\\00\\00x\\9C3\\04\\00\\002\\002<--->境外<--->瑞典<---><--->境内<--->湖北<--->电信<--->18.1<--->59.3<--->114.3<--->30.6<--->GJ<--->GZDXTOHE<--->DX<---><---><--->N/A";
//    static {
////        int messageSize = 1024 * Integer.parseInt(java.lang.String.valueOf(args[0]));
//        int messageSize = 1024*size;
//        //119.2.4.204
//        byte[] bytes = new byte[messageSize];
//        for(int i=0; i<messageSize;i++){
//            bytes[i] = '1';
//        }
//        try {
//            message = new String(bytes,"UTF-8");
//        } catch (UnsupportedEncodingException e) {
//            e.printStackTrace();
//        }
//
//        key = "119.2.4.204";
//        value = message;
//    }

    public KafkaProducerMain(int size, String topic) {
        this.size = size;
        this.topic = topic;
    }
    public void run() {
        Properties props = new Properties();
        props.put("bootstrap.servers","10.0.1.2:9092,10.0.1.3:9092,10.0.1.4:9092");
        props.put("acks","all");
        props.put("retries","0");
        props.put("linger.ms", "0");//Producer默认会把两次发送时间间隔内收集到的所有Requests进行一次聚合然后再发送
//        props.put("buffer.memory","100"); //默认是33554432，此配置项是为了解决producer端的消息堆积问题
//        props.put("batch.size","5000");//每个partition对应一个batch.size，此配置项是为了解决producer批量发送
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        Producer<String,String> producer = new KafkaProducer<String,String>(props);
        for (int i = 0; i < 2; i++) {
            producer.send(new ProducerRecord<String, String>(topic,key,value));
        }
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        producer.close();
    }


    public static void main(String[] args) {
        int size = Integer.parseInt(args[0]);
        String topic  = "test05";
        KafkaProducerMain t1 = new KafkaProducerMain(size,topic);
//        KafkaProducerMain t2 = new KafkaProducerMain(size,topic);

        t1.run();
//        t2.run();


    }

}
