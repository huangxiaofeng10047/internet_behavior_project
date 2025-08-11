package com.anryg
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.math.BigDecimal
import java.text.SimpleDateFormat
import java.util
import java.util.stream.Collectors
import java.util.{PriorityQueue, Queue}
import scala.collection.mutable
import scala.util.Random

/**
 * 模拟双11商品实时交易大屏统计分析的 Scala 实现
 */
object DoubleElementBigScreenTest {

  def main(args: Array[String]): Unit = {
    // 1. env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 2. source
    val sourceDS: DataStream[Tuple2[String, Double]] = env.addSource(new MySource())

    // 3. transformation
    val tempAggResult: DataStream[CategoryPojo] = sourceDS
      // 使用 Scala 的 keyBy 语法，`_._1` 代表 Tuple2 的第一个元素
      .keyBy(_.f0)
      .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
      .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))
      // 使用 aggregate 模式：先增量聚合 (AggregateFunction)，再全窗口处理 (ProcessWindowFunction)
      .aggregate(new PriceAggregate(), new WindowResult())


    // 4. 实现业务需求并 sink
    tempAggResult
      // `keyBy(_.dateTime)` 按照 case class 的字段进行分组，更类型安全
      .keyBy(_.dateTime)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
      // 在 ProcessWindowFunction 中处理复杂逻辑
      .process(new WindowResultProcess())
      .print("Final Result")

    // 5. execute
    env.execute("Double Eleven Big Screen")
  }

  // --- 自定义函数和 Case Class ---

  /**
   * 自定义数据源，实时产生订单数据 Tuple2<分类, 金额>
   */
  class MySource extends SourceFunction[Tuple2[String, Double]] {
    @volatile private var isRunning: Boolean = true
    private val categories = Array("女装", "男装", "图书", "家电", "洗护", "美妆", "运动", "游戏", "户外", "家具", "乐器", "办公")
    private val random = new Random()

    override def run(ctx: SourceFunction.SourceContext[Tuple2[String, Double]]): Unit = {
      while (isRunning) {
        val category = categories(random.nextInt(categories.length))
        val price = random.nextDouble() * 100
        ctx.collect(Tuple2.of(category, price))
        Thread.sleep(20)
      }
    }
    override def cancel(): Unit = isRunning = false
  }

  /**
   * Flink 的 `AggregateFunction` 用于窗口内的增量求和
   */
  class PriceAggregate extends AggregateFunction[Tuple2[String, Double], Double, Double] {
    override def createAccumulator(): Double = 0.0
    override def add(value: Tuple2[String, Double], accumulator: Double): Double = value.f1 + accumulator
    override def getResult(accumulator: Double): Double = accumulator
    override def merge(a: Double, b: Double): Double = a + b
  }

  /**
   * 用于存储聚合结果的 Scala Case Class，取代 Java 的 POJO
   */
  case class CategoryPojo(category: String, totalPrice: Double, dateTime: String)

  /**
   * ProcessWindowFunction 用于收集 `AggregateFunction` 的最终结果，并转换为 `CategoryPojo`
   */
  class WindowResult extends ProcessWindowFunction[Double, CategoryPojo, String, TimeWindow] {
    private val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    override def process(key: String, context: Context, elements: Iterable[Double], out: Collector[CategoryPojo]): Unit = {
      val price = elements.iterator.next()
      val bigDecimal = new BigDecimal(price)
      val roundPrice = bigDecimal.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue()
      val currentTimeMillis = System.currentTimeMillis()
      val dateTime = df.format(currentTimeMillis)
      out.collect(CategoryPojo(key, roundPrice, dateTime))
    }
  }

  /**
   * ProcessWindowFunction 实现最终的业务逻辑，包括计算总销售额和 Top3
   */
  class WindowResultProcess extends ProcessWindowFunction[CategoryPojo, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[CategoryPojo], out: Collector[String]): Unit = {
      // 使用 Scala 的可变优先级队列来实现小顶堆，用于高效地计算 Top3
      // Ordering.by(_.totalPrice).reverse 用于将默认的最大堆转为最小堆
      val queue: util.Queue[CategoryPojo] = new PriorityQueue[CategoryPojo](3, //初识容量
        //正常的排序,就是小的在前,大的在后,也就是c1>c2的时候返回1,也就是小顶堆
        (c1: CategoryPojo, c2: CategoryPojo) => if (c1.totalPrice >= c2.totalPrice) 1
        else -1)
      var totalPrice = 0.0

      for (element <- elements) {
        totalPrice += element.totalPrice
        if (queue.size < 3) {
          queue.add(element)
        } else if (element.totalPrice > queue.poll().totalPrice) {
          queue.clear()
          queue.add(element)
        }
      }

      val roundPrice = new BigDecimal(totalPrice).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue()

      // 对堆中的数据进行排序并格式化
//      val top3Result = queue.stream().sorted((c1: CategoryPojo, c2: CategoryPojo) => if (c1.totalPrice > c2.totalPrice) -1
//        else 1)
//        .map(c => s"(分类：${c.category} 销售总额：${c.totalPrice})")
//        .collect(Collectors.toList)
      import scala.collection.JavaConverters._
      val scalaQueue = queue.asScala.toList

      // 使用 Scala 集合操作进行排序和映射
      val top3Result = scalaQueue
        .sortBy(_.totalPrice)
        .reverse
        .map(c => s"(分类：${c.category} 销售总额：${c.totalPrice})")
        .mkString(",\n")

      val result =
        s"""时间 ： $key
           |总价 : $roundPrice
           |top3:
           |$top3Result
           |-------------""".stripMargin
      out.collect(result)
    }
  }
}
