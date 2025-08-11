package com.anryg

import java.text.SimpleDateFormat
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp
import scala.collection.mutable.ListBuffer

/*
需求：每隔5秒输出最近10分钟内访问量最多的前N个URL；与HotItems需求类似
 */
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)
case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object NetworkFlow {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val source = getClass.getResource("/apache.log")
    val dataStream = env.readTextFile(source.getPath)
      .map(line => {
        val lineArray = line.split(" ")
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = simpleDateFormat.parse(lineArray(3)).getTime
        ApacheLogEvent(lineArray(0), lineArray(2), timestamp, lineArray(5), lineArray(6))
      })
      // Time.seconds是设定延迟时间，要具体看数据本身的延迟程度；超出的可以通过窗口处理，再不行要通过SideOutput
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(t: ApacheLogEvent): Long = t.eventTime // 默认全以秒处理，这里源数据就是秒
      }) // 乱序需要定义WM
      .filter(data => {
        val pattern = "^((?!\\.(css|js|png|ico)$).)*$".r // 正则表达式，这里推荐个网站方便写正则https://regex101.com
        (pattern findFirstIn data.url).nonEmpty
      })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .aggregate(new CountAgg(), new WindowResult())

    val processedStream = dataStream
      .keyBy(_.windowEnd)
      .process(new TopNHotUrls(5))

    //    dataStream.print("aggregate")
    processedStream.print("process")

    env.execute("network flow job")
  }
}

class CountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def merge(acc: Long, acc1: Long): Long = acc + acc1

  override def getResult(acc: Long): Long = acc

  override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1
}

class WindowResult() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
  override def apply(url: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    val count = input.iterator.next
    out.collect(UrlViewCount(url, window.getEnd, count))
  }
}

class TopNHotUrls(topNum: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {
  private var urlState: ListState[UrlViewCount] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters) // 这句好像没用，我没整明白
    val urlStateDesc = new ListStateDescriptor[UrlViewCount]("urlState-state", classOf[UrlViewCount])
    urlState = getRuntimeContext.getListState(urlStateDesc)
  }

  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context,
                              out: Collector[String]): Unit = {
    urlState.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    val allUrlViews: ListBuffer[UrlViewCount] = ListBuffer()
    import scala.collection.JavaConversions._
    for (urlView <- urlState.get) { // 另一种读取方法是urlState.get().iterator()，判断它是否有hasNext，然后通过next获取
      allUrlViews += urlView
    }
    urlState.clear()
    // val sortedUrlViews = allUrlViews.sortBy(_.count)(Ordering.Long.reverse).take(topNum)
    // 另一种排序算子
    val sortedUrlViews = allUrlViews.sortWith(_.count > _.count).take(topNum)

    var result: StringBuilder = new StringBuilder
    result.append("====================================\n")
    result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")

    for (i <- sortedUrlViews.indices) {
      val currentUrlView: UrlViewCount = sortedUrlViews(i)
      result.append("No").append(i + 1).append(":").append(" URL=").append(currentUrlView.url)
        .append(" 流量=").append(currentUrlView.count).append("\n")
    }
    result.append("====================================\n\n")
    // 控制输出频率，模拟实时滚动结果
    Thread.sleep(1000)
    out.collect(result.toString)
  }
}
