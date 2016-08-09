package com.softwaremill.its

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object FlinkHotspotDetector extends App {
  Timed {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    HotspotJsFile.resetHotspotFile()

    val lines = env.readTextFile(Config.csvFileName)

    val elo = lines
      .map(_.split(","))
      .map(Trip.parseGreen _)
      .map(_.toOption)
      .flatMap(_.toTraversable)
      .filter(_.isValid)
      .addSink(new RichSinkFunction[Trip] {
        var hotspotState = HotspotState(Nil, Nil)

        val progress = new ProgressPrinter("Sink", 1000)

        override def invoke(value: Trip) = {
          hotspotState = hotspotState.addTrip(value)
          progress.inc()
        }

        override def close() = {
          val hotspots = hotspotState.result()
          val sortedHotspots = hotspots.sortBy(_.count)
          sortedHotspots.foreach(println)
          println(s"Found ${hotspots.size} hotspots")
          HotspotJsFile.saveHotspotsAsJsData(hotspots)
        }
      })

    env.execute()
  }
}

