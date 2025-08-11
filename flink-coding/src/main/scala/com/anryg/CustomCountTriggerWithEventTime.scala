package com.anryg
//学习valuestate
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{MapFunction, ReduceFunction}
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor}
import org.apache.flink.api.common.typeutils.base.LongSerializer
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import java.time.Duration

// 实体类
case class CartInfo(sensorId: String, count: Int, datetime: Long)

// 自定义触发器
class CustomCountTriggerWithEventTime[T](maxCount: Long) extends Trigger[T, TimeWindow] {
  private val countStateDescriptor = new ReducingStateDescriptor[Long]("countState", new ReduceSum, createTypeInformation[Long]);

  def fireAndPurge(timestamp: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult = {
    clear(window, ctx)
    TriggerResult.FIRE_AND_PURGE
  }

  override def onElement(element: T, timestamp: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult = {
    if (window.maxTimestamp > ctx.getCurrentWatermark) {
      ctx.registerEventTimeTimer(window.maxTimestamp)
    }
    val countState = ctx.getPartitionedState(countStateDescriptor)
    countState.add(1L)
    if (countState.get() >= maxCount) {
      fireAndPurge(timestamp, window, ctx)
    } else {
      TriggerResult.CONTINUE
    }
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult = {
    if (time >= window.maxTimestamp) {
      fireAndPurge(time, window, ctx)
    } else {
      TriggerResult.CONTINUE
    }
  }

  override def onEventTime(time: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult = {
    if (time == window.maxTimestamp) {
      fireAndPurge(time, window, ctx)
    } else {
      TriggerResult.CONTINUE
    }
  }

  override def clear(window: TimeWindow, ctx: TriggerContext): Unit = {
    ctx.deleteEventTimeTimer(window.maxTimestamp)
    val countState = ctx.getPartitionedState(countStateDescriptor)
    countState.clear()
  }

  override def canMerge: Boolean = true

  override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = {
    val windowMaxTimestamp = window.maxTimestamp
    if (windowMaxTimestamp > ctx.getCurrentWatermark) {
      ctx.registerEventTimeTimer(windowMaxTimestamp)
    }
  }

  class ReduceSum extends ReduceFunction[Long] {
    override def reduce(value1: Long, value2: Long): Long = value1 + value2
  }
}

object FlinkCustomTriggerScala {
  def main(args: Array[String]): Unit = {
    // 0.env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)
    env.setParallelism(1)

    // 1.source
    val lines = env.socketTextStream("127.0.0.1", 9999)
    lines.print()

    // 2.transformation
    val souceDS = lines.map(new MapFunction[String, CartInfo] {
      override def map(value: String): CartInfo = {
        val arr = value.split(",")
        CartInfo(arr(0), arr(1).toInt, arr(2).toLong)
      }
    }).assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(1))
      .withTimestampAssigner(new SerializableTimestampAssigner[CartInfo] {
        override def extractTimestamp(element: CartInfo, recordTimestamp: Long): Long = element.datetime * 1000
      }))

    val keyedDS = souceDS.keyBy(_.sensorId)

    // 需求1:每5秒钟统计一次，最近5秒钟内，各个SensorId的count数量之和--基于时间的滚动窗口
    val result = keyedDS
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .trigger(new CustomCountTriggerWithEventTime[CartInfo](3))
      .reduce(new ReduceFunction[CartInfo] {
        override def reduce(cartInfo: CartInfo, t1: CartInfo): CartInfo = {
          CartInfo(cartInfo.sensorId, cartInfo.count + t1.count, t1.datetime)
        }
      })

    // 3.sink
    result.print("test")

    // 4.execute
    env.execute()
  }
}



