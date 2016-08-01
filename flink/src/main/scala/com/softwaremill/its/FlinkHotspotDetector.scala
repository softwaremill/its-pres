package com.softwaremill.its

import java.util.Date

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object FlinkHotspotDetector extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  HotspotJsFile.resetHotspotFile()

  val start = new Date()

  val lines = env.readTextFile(Config.csvFileName)

  val elo = lines
    .map(_.split(","))
    .map(Trip.parseGreen _)
    .map(_.toOption)
    .flatMap(_.toTraversable)
    .filter(_.isValid)
    .addSink(new RichSinkFunction[Trip] {
      var hotspotState = HotspotState(Nil, Nil)

      var counter = 1

      override def invoke(value: Trip) = {
        hotspotState = hotspotState.addTrip(value)
        counter = counter + 1
        if (counter % 1000 == 0) println(s"Processing trip $counter")
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

  println(s"Finished in ${new Date().getTime - start.getTime} ms")
}

