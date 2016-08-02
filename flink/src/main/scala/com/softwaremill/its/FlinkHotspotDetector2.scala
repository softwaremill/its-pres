package com.softwaremill.its

import java.time.{Instant, OffsetDateTime, ZoneOffset}
import java.util.Date
import java.util.concurrent.atomic.AtomicInteger

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object FlinkHotspotDetector2 {

  var maxDate = OffsetDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)
  val c = new AtomicInteger()

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    HotspotJsFile.resetHotspotFile()

    val start = new Date()

    println("Reading from: " + Config.csvFileName)
    println("Writing to: " + Config.HotspotFile)
    val lines = env.readTextFile(Config.csvFileName)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    lines
      .map(_.split(","))
      .map(Trip.parseGreen _)
      .flatMap(_.toOption)
      .filter(_.isValid)
      .assignAscendingTimestamps(t => t.dropoffTime.toEpochSecond * 1000L)

      // pre-aggregation
      .flatMap(toBoxes _)
      .keyBy(_.serialize)
      .timeWindow(Time.minutes(WindowBounds.StepLengthMinutes))
      .fold((0, GridBox(0, 0))) { case ((count, _), gb) => (count+1, gb) }

      //
      .flatMap(GridBoxCounts.forBoxesWithCounts _)
      .keyBy(_.gb.serialize)
      .timeWindow(Time.minutes(WindowBounds.WindowLengthMinutes), Time.minutes(WindowBounds.StepLengthMinutes))
      .trigger(EventTimeTrigger.create())
      .apply((_: GridBoxCounts).add(_: GridBoxCounts), gridBoxCountsWindowFunction)
//      .addSink { t =>
//        val r = c.addAndGet(1)
//        if (r%1000 == 0) println(r + " " + c.hashCode())
//      }
      .flatMap(_.detectHotspot)
      .addSink { hotspot =>
        println(hotspot)
      }

    //      .keyBy(_ => 0)
    //      .fold(List.empty[Hotspot])((r, h) => h :: r)
    //      .addSink { hotspots =>
    //        val sortedHotspots = hotspots.sortBy(_.count)
    //        sortedHotspots.foreach(println)
    //        println(s"Found ${hotspots.size} hotspots")
    //        HotspotDetector.saveHotspotsAsJsData(hotspots)
    //      }

    env.execute()

    println(s"Finished in ${new Date().getTime - start.getTime} ms")
  }

  def gridBoxCountsWindowFunction(gb: String, tw: TimeWindow, gbcs: Iterable[GridBoxCounts], collector: Collector[AddedCountsWithWindow]): Unit = {
    val addedCounts = gbcs.reduce(_.add(_))
    val start = Instant.ofEpochMilli(tw.getStart).atOffset(ZoneOffset.UTC)
    val end = Instant.ofEpochMilli(tw.getEnd).atOffset(ZoneOffset.UTC)
    val result = AddedCountsWithWindow(addedCounts.gb, addedCounts.counts, WindowBounds(start, end))

    if (end.isAfter(maxDate)) {
      maxDate = end
      println("WINDOW MAX: " + maxDate)
    }

    collector.collect(result)
    collector.close()
  }

  def toBoxes(t: Trip): List[GridBox] = {
    GridBox.boxesFor(t.dropoffLat, t.dropoffLng)
  }
}

