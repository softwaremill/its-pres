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
      .keyBy(gb => s"${gb.x},${gb.y}")
      .timeWindow(Time.minutes(WindowBounds.StepLengthMinutes))
      .fold((0, GridBox(0, 0))) { case ((count, _), gb) => (count+1, gb) }

      //
      .flatMap(toCounts2 _)
      .keyBy(gbc => s"${gbc.gb.x},${gbc.gb.y}")
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

  def toCounts(t: Trip): List[GridBoxCounts] = {
    val boxes = GridBox.boxesFor(t.dropoffLat, t.dropoffLng)
    boxes.flatMap { b =>
      List(
        GridBoxCounts.forGridBox(b),
        GridBoxCounts.forGridBoxNeighbor(GridBox(b.x + 5, b.y - 5), 1),
        GridBoxCounts.forGridBoxNeighbor(GridBox(b.x + 5, b.y + 0), 2),
        GridBoxCounts.forGridBoxNeighbor(GridBox(b.x + 5, b.y + 5), 3),
        GridBoxCounts.forGridBoxNeighbor(GridBox(b.x + 0, b.y + 5), 4),
        GridBoxCounts.forGridBoxNeighbor(GridBox(b.x + 0, b.y - 5), 5),
        GridBoxCounts.forGridBoxNeighbor(GridBox(b.x - 5, b.y - 5), 6),
        GridBoxCounts.forGridBoxNeighbor(GridBox(b.x - 5, b.y + 0), 7),
        GridBoxCounts.forGridBoxNeighbor(GridBox(b.x - 5, b.y + 5), 8)
      )
    }
  }

  def toBoxes(t: Trip): List[GridBox] = {
    GridBox.boxesFor(t.dropoffLat, t.dropoffLng)
  }

  def toCounts2(i: (Int, GridBox)): List[GridBoxCounts] = {
    val count = i._1
    val b = i._2
    List(
      GridBoxCounts.forGridBox(b, count),
      GridBoxCounts.forGridBoxNeighbor(GridBox(b.x + 5, b.y - 5), 1, count),
      GridBoxCounts.forGridBoxNeighbor(GridBox(b.x + 5, b.y + 0), 2, count),
      GridBoxCounts.forGridBoxNeighbor(GridBox(b.x + 5, b.y + 5), 3, count),
      GridBoxCounts.forGridBoxNeighbor(GridBox(b.x + 0, b.y + 5), 4, count),
      GridBoxCounts.forGridBoxNeighbor(GridBox(b.x + 0, b.y - 5), 5, count),
      GridBoxCounts.forGridBoxNeighbor(GridBox(b.x - 5, b.y - 5), 6, count),
      GridBoxCounts.forGridBoxNeighbor(GridBox(b.x - 5, b.y + 0), 7, count),
      GridBoxCounts.forGridBoxNeighbor(GridBox(b.x - 5, b.y + 5), 8, count)
    )
  }
}

/**
  * @param counts In the map, key `0` is the count for the grid box itself. Other keys are the grid box's neighbors.
  */
case class GridBoxCounts(gb: GridBox, counts: Array[Int]) {
  def add(gbc: GridBoxCounts): GridBoxCounts = {
    val addedCounts = Array.ofDim[Int](9)
    var i = 0
    while (i < 9) {
      addedCounts(i) = counts(i) + gbc.counts(i)
      i += 1
    }
    GridBoxCounts(gb, addedCounts)
  }
}

object GridBoxCounts {
  def forGridBox(gb: GridBox, count: Int = 1) = {
    val a = Array.ofDim[Int](9)
    a(0) = count
    GridBoxCounts(gb, a)
  }
  def forGridBoxNeighbor(gb: GridBox, nghNumber: Int, count: Int = 1) = {
    val a = Array.ofDim[Int](9)
    a(nghNumber) = count
    GridBoxCounts(gb, a)
  }
}

case class AddedCountsWithWindow(gb: GridBox, counts: Array[Int], bounds: WindowBounds) {
  def detectHotspot: Option[Hotspot] = {
    val centerCount = counts(0)

    import Window._

    if (centerCount < CountThreshold) None else {
      val neighborCounts = counts.toList.tail

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