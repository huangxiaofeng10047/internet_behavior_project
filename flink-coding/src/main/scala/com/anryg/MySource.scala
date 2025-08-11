package com.anryg

import org.apache.calcite.avatica.com.google.protobuf.SourceContext

import scala.util.Random

/**
 * 自定义数据源实时产生订单数据Tuple2<分类, 金额>
 */
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.api.java.tuple.Tuple2
import scala.util.Random

/**
 * Custom data source that generates real-time order data Tuple2<Category, Amount>
 */
class MySource extends SourceFunction[Tuple2[String, Double]] {

  private var isRunning: Boolean = true
  private val categories = Array("女装", "男装", "图书", "家电", "洗护", "美妆", "运动", "游戏", "户外", "家具", "乐器", "办公")
  private val random = new Random()

  override def run(ctx: SourceFunction.SourceContext[Tuple2[String, Double]]): Unit = {
    while (isRunning) {
      // Randomly generate category and amount
      val category = categories(random.nextInt(categories.length))
      val price = random.nextDouble() * 100 // Note: nextDouble generates a random number in [0, 1)

      ctx.collect(Tuple2.of(category, price))
      Thread.sleep(20)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}


