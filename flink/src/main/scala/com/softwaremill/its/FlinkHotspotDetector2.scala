package com.softwaremill.its

import java.time.{OffsetDateTime, ZoneOffset}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger

object FlinkHotspotDetector2 {

  var maxDate = OffsetDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)
  val c = new AtomicInteger()

  def main(args: Array[String]): Unit = Timed {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    HotspotJsFile.resetHotspotFile()

    println("Reading from: " + Config.csvFileName)
    println("Writing to: " + Config.HotspotFile)
    val lines = env.readTextFile(Config.csvFileName)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    lines
      .flatMap(Trip.parseOrDiscard _)
      .assignAscendingTimestamps(t => t.dropoffTime.toEpochSecond * 1000L)
      .flatMap { t =>
        WindowBounds.boundsFor(t.dropoffTime).map(_ -> t)
      }
      .keyBy(_._1)
      .timeWindow(Time.minutes(WindowBounds.WindowLengthMinutes), Time.minutes(WindowBounds.StepLengthMinutes))
      .trigger(EventTimeTrigger.create())
      .fold(Window.Empty) { case (w, (wb, t)) =>
        w.copy(bounds = wb).addTrip(t)
      }
      //      .addSink { t =>
      //        val r = c.addAndGet(1)
      //        if (r%1000 == 0) println(r + " " + c.hashCode())
      //      }
      .flatMap(_.close())
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
  }
}

