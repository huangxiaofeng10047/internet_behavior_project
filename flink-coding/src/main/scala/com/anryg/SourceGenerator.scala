package com.anryg

import com.alibaba.fastjson.{JSON, JSONObject}

import java.io.{FileWriter, PrintWriter}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties
import scala.collection.mutable.ListBuffer

object SourceGenerator {
  private val SPEED = 1000L // 每秒1000条

  case class InternetBehavior(id:String, client_ip:String, domain:String, do_time:String, target_ip:String,rcode:String, query_type:String, authority_record:String, add_msg:String, dns_ip:String)//定义当前数据对象

  def main(args: Array[String]): Unit = {
    var speed: Long = SPEED
    args.foreach(println)
    if (args.length > 0) speed = args(0).toLong
    val delay = 1000000 / speed // 每条耗时多少毫秒
    import scala.io.{Source, StdIn}


    try {
      Thread.currentThread().getContextClassLoader.getResource("inter_user_behavior.log").getPath
      val inputStream = Source.fromFile(Thread.currentThread().getContextClassLoader.getResource("inter_user_behavior.log").getPath
      )
        val lines = inputStream.getLines()
        var start = System.nanoTime
      val list:ListBuffer[InternetBehavior] = ListBuffer()
         while (lines.hasNext){
           lines.drop(1)
           val line=lines.next()

          println(line)
          //跳过第一行处理，第二行开始处理数据
           val array =line.split(",");
           val seed=InternetBehavior(array(0)+array(1)+array(2),array(0),array(1),array(2),array(3),array(4),array(5),array(6),array(7),array(8))
           //生成十万条数据把
           val writer = new PrintWriter(new FileWriter("example.txt", true))

           for (i <- 1 to 10000){
             val sdf = new SimpleDateFormat("yyyyMMddhhmmss")
             // 获取当前时间
             val currentDateTime = LocalDateTime.now()

             // 定义时间格式
             val formatter = DateTimeFormatter.ofPattern("yyyyMMddhhmmss")

             // 格式化时间字符串
             val formattedDateTime = currentDateTime.format(formatter)

             // 打印时间字符串
             println(formattedDateTime)
             val internetBehavior=InternetBehavior(
               seed.id+""+i.toString,
               seed.client_ip+i.toString,
               seed.domain+i.toString,
               formattedDateTime,
               seed.target_ip+i.toString,
               seed.rcode+i.toString,
               seed.query_type+i.toString,
               seed.authority_record+i.toString,
                seed.add_msg+i.toString, seed.dns_ip+i.toString
             )
             list.append(internetBehavior)
             println(internetBehavior.toString)
             writer.append("\n"+internetBehavior.toString)
           }

           writer.close()

           var end = System.nanoTime
          var diff = end - start
          while (diff < (delay * 1000)) {
            Thread.sleep(1)
            end = System.nanoTime
            diff = end - start
          }
          start = end
        }
      // 设置 Kafka 生产者配置
      val props = new Properties()
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:39092,localhost:39093,localhost:39094") // Kafka brokers 地址
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer") // 键的序列化器
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer") // 值的序列化器

      // 创建 Kafka 生产者
      val producer = new KafkaProducer[String, String](props)

      // 要发送的消息
      val topic = "test-time"
      val messageKey = "message-key"
      val messageValue = "Hello, Kafka!"

      // 创建 ProducerRecord
      val record = new ProducerRecord[String, String](topic, messageKey, messageValue)

      try {
        // 发送消息
        for (inter<-list){
//          println(inter.toString)
          val sb=new StringBuffer("")
          sb.append(inter.client_ip).append(",").append(inter.domain)
            .append(",").append(inter.do_time).append(",")
            .append(inter.target_ip).append(",")
            .append(inter.rcode).append(",")
            .append(inter.query_type).append(",")
            .append(inter.authority_record).append(",")
            .append(inter.add_msg).append(",")
            .append(inter.dns_ip)
          val jsonObject = new JSONObject()
          jsonObject.put("message",sb.toString)
          val record1=new ProducerRecord[String, String](topic, messageKey, jsonObject.toString)
          val future = producer.send(record1)
          // 等待发送结果 (可选，可以异步发送)
          val metadata = future.get()
          println(s"发送消息到 topic: ${metadata.topic()}, partition: ${metadata.partition()}, offset: ${metadata.offset()}")

        }
         } catch {
        case e: Exception =>
          e.printStackTrace()
      } finally {
        // 关闭生产者
        producer.close()
      }
      }
    }

  }
