package com.softwaremill.its

import java.time.{LocalDateTime, OffsetDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import better.files.File

import scala.util.Try

object Config {
  val Month = "10"
  val Green = true
  val HotspotFile = s"src/main/resources/hotspots-${if (Green) "green" else "yellow"}-$Month.js"
  val CsvFile = "files"

  def csvFileName = s"$CsvFile/sorted_${if (Green) "green" else "yellow"}_tripdata_2015-$Month.csv"
}

object HotspotJsFile {
  def resetHotspotFile(): Unit = {
    val f = File(Config.HotspotFile)
    f.overwrite("")
    f.appendLine("var hotspots = [];")
  }

  def saveHotspotsAsJsData(hotspots: List[Hotspot]): Unit = {
    if (hotspots.nonEmpty) {
      val f = File(Config.HotspotFile)

      f.appendLine("hotspots = hotspots.concat([")
      hotspots.foreach { hotspot =>
        f.appendLine(s"{" +
          s"lat: ${hotspot.box.center._1}, " +
          s"lng: ${hotspot.box.center._2}, " +
          s"count: ${hotspot.count}, " +
          s"""bounds: { start: "${hotspot.bounds.startAsString}", end: "${hotspot.bounds.endAsString}" }, """ +
          s"""nghsCount: "${hotspot.neighborCounts.mkString(",")}" },""")
      }
      f.appendLine("]);")
    }
  }
}

case class Trip(pickupLat: Double, pickupLng: Double, dropoffLat: Double, dropoffLng: Double,
  distance: Double, dropoffTime: OffsetDateTime) {

  def isValid = isInNy(pickupLat, pickupLng) && isInNy(dropoffLat, dropoffLng) && distance > 0.1

  def isInNy(lat: Double, lng: Double) = lat > 39.0 && lat < 43.0 && lng > -75.0 && lng < 71.0
}

object Trip {
  val DateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  def parseGreen(in: Array[String]) = Try {
    Trip(in(6).toDouble, in(5).toDouble, in(8).toDouble, in(7).toDouble, in(10).toDouble,
      LocalDateTime.parse(in(2), DateFormat).atOffset(ZoneOffset.of("-05:00"))) // EST
  }

  def parseYellow(in: Array[String]) = Try {
    Trip(in(6).toDouble, in(5).toDouble, in(10).toDouble, in(9).toDouble, in(10).toDouble,
      LocalDateTime.parse(in(2), DateFormat).atOffset(ZoneOffset.of("-05:00"))) // EST
  }
}

// A box spans from <x, y> to (x+step, y+step). Left and top borders are inclusive, right- and bottom are exclusive.
case class GridBox(x: Int, y: Int) {

  import GridBox._

  def center: (Double, Double) = ((x + BoxUnits / 2.0d) * LatUnit, (y + BoxUnits / 2.0d) * LngUnit)

  override def toString = {
    val (lat, lng) = center
    f"($lat%2.6f, $lng%2.6f)"
  }
}

object GridBox {
  // each degree of latitude is approximately 69 miles (111 kilometers) apart; 100m = 0.0009
  val LatUnit = 0.0009

  // At 40° north or south the distance between a degree of longitude is 53 miles (85 km); 100m = 0.0012
  val LngUnit = 0.0012

  // Grid boxes have the given number of units width & height. They are spaced by the given step (if step < box units,
  // then each point belongs to a number of boxes)
  val BoxUnits = 5
  val StepUnits = 1

  val StepLengthLan = LatUnit * StepUnits
  val StepLengthLng = LngUnit * StepUnits

  val BoxLengthLan = LatUnit * BoxUnits
  val BoxLengthLng = LngUnit * BoxUnits

  def boxesFor(lat: Double, lng: Double): List[GridBox] =
    (for {
      x <- math.floor((lat - BoxLengthLan) / StepLengthLan + 1).toInt.to(math.floor(lat / StepLengthLan).toInt)
      y <- math.floor((lng - BoxLengthLng) / StepLengthLng + 1).toInt.to(math.floor(lng / StepLengthLng).toInt)
    } yield GridBox(x, y)).toList
}

/**
  * @param start Inclusive
  * @param end   Exclusive
  */
case class WindowBounds(start: OffsetDateTime, end: OffsetDateTime) {
  def contains(time: OffsetDateTime) = !start.isAfter(time) && end.isAfter(time)

  def startAsString = Trip.DateFormat.format(start)

  def endAsString = Trip.DateFormat.format(end)
}

object WindowBounds {
  val WindowLengthMinutes = 30
  val StepLengthMinutes = 5

  val WindowsPerTimepoint = WindowLengthMinutes / StepLengthMinutes

  def boundsFor(t: OffsetDateTime): List[WindowBounds] = {
    val firstBoundMinute = t.getMinute - t.getMinute % StepLengthMinutes
    val firstBoundStart = t.withMinute(firstBoundMinute).withSecond(0).withNano(0)
    (for (i <- 0 until WindowsPerTimepoint) yield
      WindowBounds(
        firstBoundStart.plusMinutes(i * StepLengthMinutes),
        firstBoundStart.plusMinutes(i * StepLengthMinutes + WindowLengthMinutes)
      )).toList
  }
}

case class Hotspot(bounds: WindowBounds, box: GridBox, count: Int, neighborCounts: List[Int])

case class Window(bounds: WindowBounds, boxCounts: Map[GridBox, Int]) {

  import Window._

  def addBox(gb: GridBox): Window = copy(boxCounts = boxCounts.updated(gb, boxCounts.getOrElse(gb, 0) + 1))

  def close(): List[Hotspot] = {
    boxCounts.flatMap { case (box, count) =>
      if (count < CountThreshold) None
      else {
        val neighborCounts = NeighborhoodOffsets.map { offset =>
          val neighborBox = box.copy(box.x + offset._1, box.y + offset._2)
          boxCounts.getOrElse(neighborBox, 0)
        }

        if (neighborCounts.forall(nghCount => nghCount * NeighborsMultiplierThreshold <= count)) {
          println("HOTSPOT " + count)
          val hotspot = Hotspot(bounds, box, count, neighborCounts)
          Some(hotspot)
        } else {
          println("NON-HOTSPOT CANDIDATE " + count)
          None
        }
      }
    }
  }.toList
}

object Window {
  val CountThreshold = if (Config.Green) 50 else 150
  val NeighborsMultiplierThreshold = if (Config.Green) 2.0d else 2.5d
  val NeighborhoodOffsets: List[(Int, Int)] = {
    import GridBox._
    val offsets = List(-BoxUnits, 0, BoxUnits)
    for {
      xo <- offsets
      yo <- offsets
      if !(xo == 0 && yo == 0)
    } yield (xo, yo)
  }
}

case class HotspotState(openWindows: List[Window], detectedHotspots: List[Hotspot]) {
  def addTrip(t: Trip): HotspotState = {
    val (openWindows2, newHotspots) = closePastWindows(t.dropoffTime, openWindows)
    val openWindows3 = createNewWindows(t.dropoffTime, openWindows2)
    val openWindows4 = addToOpenWindows(t, openWindows3)

    copy(
      openWindows = openWindows4,
      detectedHotspots = newHotspots ++ detectedHotspots
    )
  }

  def result(): List[Hotspot] = openWindows.flatMap(_.close()) ++ detectedHotspots

  private type CloseResult = (List[Window], List[Hotspot])

  private def closePastWindows(time: OffsetDateTime, wnds: List[Window]): CloseResult = {
    wnds.foldLeft[CloseResult]((Nil, Nil)) { case ((openWnds, hotspots), window) =>
      if (window.bounds.contains(time)) {
        (window :: openWnds, hotspots)
      } else {
        (openWnds, window.close() ++ hotspots)
      }
    }
  }

  private def createNewWindows(time: OffsetDateTime, wnds: List[Window]): List[Window] = {
    val bounds = WindowBounds.boundsFor(time)
    val currentBounds = wnds.map(_.bounds).toSet
    val newBounds = bounds.filterNot(currentBounds.contains)
    newBounds.map(Window(_, Map())) ++ wnds
  }

  private def addToOpenWindows(t: Trip, wnds: List[Window]): List[Window] = {
    val boxes = GridBox.boxesFor(t.dropoffLat, t.dropoffLng)
    wnds.map(w => boxes.foldLeft(w) { case (w2, b) => w2.addBox(b) })
  }
}