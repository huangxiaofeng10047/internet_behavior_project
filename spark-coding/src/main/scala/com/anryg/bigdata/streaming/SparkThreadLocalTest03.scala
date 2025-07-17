package com.anryg.bigdata.streaming

import com.anryg.bigdata.RedisClientUtilsV2
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @DESC: 测试Spark利用Redis的连接单例 + ThreadLocal来引入外部数据源
 * @Auther: Anryg
 * @Date: 2024/7/9 17:45
 */
object SparkThreadLocalTest03 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkThreadLocalTest03")
      .setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    /**构造数据源*/
    val seq = Seq("baidu.com",
      "qq.com",
      "xiaomi.com",
      "12345.com",
      "abc.com",
      "def.net",
      "xxx.com",
      "google.com",
      "sina.com",
      "163.com",
      "yyy.net",
      "360.com",
      "cctv.cn",
      "12306.com")

    spark.sparkContext.makeRDD(seq) /**生成 RDD 对象*/
      .foreach(domain => {
        /**通过ThreadLocal获取Redis连接，并拿到黑名单数据*/
        val blackDomainSet = RedisClientUtilsV2.getSetResult(RedisClientUtilsV2.getRedisThreadLocal.get(),
          5,
          "black_domain")

        if (!blackDomainSet.contains(domain)) {
          Thread.sleep(5000) /**为了方便观察Redis的连接情况，sleep几秒*/
          println(domain)
        }
      })
  }
}
