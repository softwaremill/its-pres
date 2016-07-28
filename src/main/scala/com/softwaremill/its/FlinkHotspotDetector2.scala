package com.softwaremill.its

import java.time.{Instant, LocalDateTime, OffsetDateTime, ZoneOffset}
import java.util.Date

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

object FlinkHotspotDetector2 {

  var maxDate = OffsetDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    HotspotDetector.resetHotspotFile()

    val start = new Date()

    println("Reading from: " + HotspotDetector.getCsvFileName)
    println("Writing to: " + Files.HotspotFile)
    val lines = env.readTextFile(HotspotDetector.getCsvFileName)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)



    val elo = lines
      .map(_.split(","))
      .map(Trip.parseGreen _)
      .flatMap(_.toOption)
      .filter(_.isValid)
      .assignAscendingTimestamps(t => t.dropoffTime.toEpochSecond * 1000L)
      .flatMap(toCounts _)
      // pre-aggregation
      .keyBy(_.gb.toString)
      .timeWindow(Time.minutes(WindowBounds.StepLengthMinutes))
      .reduce((_: GridBoxCounts).add(_: GridBoxCounts))
      //
      .keyBy(_.gb.toString)
      .timeWindow(Time.minutes(WindowBounds.WindowLengthMinutes), Time.minutes(WindowBounds.StepLengthMinutes))
      .trigger(EventTimeTrigger.create())
      .apply((_: GridBoxCounts).add(_: GridBoxCounts), gridBoxCountsWindowFunction)
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

  def toCounts(t: Trip): List[GridBoxCounts] = {
    val boxes = GridBox.boxesFor(t.dropoffLat, t.dropoffLng)
    boxes.flatMap { b =>
      val nghs = for ((xo, yo) <- Window.NeighborhoodOffsets) yield {
        val ngh = GridBox(b.x + xo, b.y + yo)
        // some encoding of a pair of numbers into a single number
        // we use -xo and -yo for clarity as these are the offsets of the current box from the neighbors point of view
        GridBoxCounts.forGridBoxNeighbor(ngh, 100*(-xo) + (- yo))
      }

      GridBoxCounts.forGridBox(b) :: nghs
    }
  }
}

/**
  * @param counts In the map, key `0` is the count for the grid box itself. Other keys are the grid box's neighbors.
  */
case class GridBoxCounts(gb: GridBox, counts: Map[Int, Int]) {
  def add(gbc: GridBoxCounts): GridBoxCounts = {
    val addedCounts = mutable.Map.empty[Int, Int]
    counts.foreach(t => addedCounts.put(t._1, t._2))
    gbc.counts.foreach(t => addedCounts.put(t._1, t._2 + addedCounts.getOrElse(t._1, 0)))
    GridBoxCounts(gb, addedCounts.toMap)
  }
}

object GridBoxCounts {
  def forGridBox(gb: GridBox) = GridBoxCounts(gb, Map(0 -> 1))
  def forGridBoxNeighbor(gb: GridBox, nghNumber: Int) = GridBoxCounts(gb, Map(nghNumber -> 1))
}

case class AddedCountsWithWindow(gb: GridBox, counts: Map[Int, Int], bounds: WindowBounds) {
  def detectHotspot: Option[Hotspot] = {
    val centerCount = counts.getOrElse(0, 0)

    import Window._

    if (centerCount < CountThreshold) None else {
      val neighborCounts = counts.filter(_._1 != 0).values.toList

      if (neighborCounts.forall(nghCount => nghCount * NeighborsMultiplierThreshold <= centerCount)) {
        println("HOTSPOT " + centerCount)

        Some(Hotspot(bounds, gb, centerCount, neighborCounts))
      } else {
        println("NON-HOTSPOT CANDIDATE " + centerCount)
        None
      }
    }
  }
}