package com.softwaremill.its

import java.time.{LocalDateTime, OffsetDateTime, ZoneOffset}
import java.util.Date

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object FlinkHotspotDetector extends App {

  import Trip._

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  HotspotDetector.resetHotspotFile()

  val start = new Date()

  implicit def convertStringToOffsetDateTime(s: String): OffsetDateTime =
    LocalDateTime.parse(s, DateFormat).atOffset(ZoneOffset.of("-05:00"))

  val lines = env.readTextFile(HotspotDetector.getCsvFileName)

  val elo = lines
    .map(_.split(","))
    .map(Trip.parseGreen(_))
    .map(_.toOption)
    .filter(_.isDefined)
    .map(_.get)
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
        HotspotDetector.saveHotspotsAsJsData(hotspots)
      }
    })

  env.execute()

  println(s"Finished in ${new Date().getTime - start.getTime} ms")
}

