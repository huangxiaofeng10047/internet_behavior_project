package com.anryg.bigdata.streaming
import com.anryg.bigdata.{IpSearch, RedisClientUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.alibaba.fastjson.JSON
import java.util.concurrent.TimeUnit
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
object StreamingWithThreadLocal {
  def main(args:Array[String]): Unit = {
    val conf=new SparkConf() .setAppName("StreamingWithThreadLocal") .setMaster("local[*]")
    //本地运行模式，如果提交集群，注释掉这行
    val spark=SparkSession.builder().config(conf).getOrCreate()
    val rawDF=spark.readStream .format("kafka")
   //确定数据源的来源格式
      .option("kafka.bootstrap.servers","192.168.211.107:6667")
   //指定kafka集群的地址，理论上写一个broker就可以了
   .option("subscribe","test") //指定topic
   // .option("group.id","test9999")
  /**不再用该方式来绑定offset，
   * 而是每个程序有个唯一的id，
   * 该id跟checkpointLocation绑定，
   * 虽然group.id属性在运行中依然保留，但是不再跟offset绑定
   * */
   .option("failOnDataLoss",false)//如果读取数据源时，发现数据突然缺失，比如被删，则是否马上抛出异常
   .option("fetchOffset.numRetries",3)//获取消息的偏移量时，最多进行的重试次数
   .option("maxOffsetsPerTrigger",500)
   /**用于限流，限定每次读取数据的最大条数，不指定则是asfastaspossible*/
   .option("startingOffsets","earliest")//第一次消费时，读取kafka数据的位置
  .load()
    import spark.implicits._
    val ds=rawDF.selectExpr("CAST(value AS STRING)")//将kafka中的数据的value转为为string，原始为binary类型
   .map(row=>{ val line=row.getAs[String]("value")//获取row对象中的field，其实也只有一个field
   val rawJson=JSON.parseObject(line)//原始string是一个json，对其进行解析
   val message=rawJson.getString("message")//获取业务数据部分
   val msgArray=message.split(",")//指定分隔符进行字段切分
      msgArray
 }).filter(_.length==9)//只留字段数为9的数据
 .filter(array=>array(2).length>=8)//确保日期字段符合规范
 .map(array=>{ val ipAndAddr=IpSearch.getAddrByIP(RedisClientUtils.getSingleRedisClient,array(0))//使用普通单例方式管理Jedis的模式
// val ipAndAddr=IpSearch.getAddrByIP(RedisClientUtilsV2.getRedisThreadLocal.get(),array(0))
// 使用ThreadLocal方式管理Jedis的模式
(array(0)+array(1)+array(2),array(0),array(1),ipAndAddr) })//将其转化成为元组，为了方便下一步赋予schema
.toDF("id","client_ip","domain","ip_addr")  //给裸数据添加字段名
 ds.printSchema()//打印schema，确认没有问题
 val query=ds.writeStream
   .outputMode(OutputMode.Append())//指定数据的写入方式
   .format("console")
   .trigger(Trigger.ProcessingTime(10,TimeUnit.SECONDS))
   /**每60秒执行一次，不指定就是asfastaspossible*/
   .option("checkpointLocation","hdfs://192.168.211.106:8020/tmp/offset/test/StreamingWithThreadLocal")
   /**用来保存offset，用该目录来绑定对应的offset，如果该目录发生改变则程序运行的id会发生变化，类比group.id的变化，写hive的时候一定不要轻易变动*/
   .start()
   query.awaitTermination()
 }
  }
