package com.softwaremill.its

import java.nio.file.Paths
import java.util.Date

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{FileIO, Framing, Source}
import akka.util.ByteString

import scala.collection.mutable
import scala.concurrent.Await

object HotspotDetector2 {
  def main(args: Array[String]): Unit = {

    implicit val as = ActorSystem()
    implicit val mat = ActorMaterializer()

    HotspotJsFile.resetHotspotFile()

    val start = new Date()

    val f = FileIO.fromPath(Paths.get(Config.csvFileName))
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024))
      .map(_.utf8String)
      .map(_.split(","))
      .map(Trip.parse)
      .map(_.toOption)
      .collect { case Some(x) => x }
      .filter(_.isValid)
      .zip(Source.unfold(0)(st => Some((st + 1, st + 1))))
      .map { case (t, i) =>
        if (i % 1000 == 0) println(s"Processing trip $i")
        t
      }
      .async
      .statefulMapConcat { () =>
        val openWindows = mutable.Set[WindowBounds]()
        t => {
          val bounds = WindowBounds.boundsFor(t.dropoffTime).toSet
          val closeCommands = openWindows.toList.flatMap { ow =>
            if (!bounds.contains(ow)) {
              openWindows.remove(ow)
              Some(CloseWindow(ow))
            } else None
          }
          val openCommands = bounds.flatMap { wb =>
            if (!openWindows.contains(wb)) {
              openWindows.add(wb)
              Some(OpenWindow(wb))
            } else None
          }
          val addCommands = bounds.map(wb => AddToWindow(t, wb))

          openCommands ++ closeCommands ++ addCommands
        }
      }
      .async
      .groupBy(1024, _.wb)
      .takeWhile(!_.isInstanceOf[CloseWindow])
      .fold(Window(null, Map())) {
        case (w, OpenWindow(wb)) => w.copy(bounds = wb)
        case (w, CloseWindow(_)) => w
        case (w, AddToWindow(t, wb)) =>
          val boxes = GridBox.boxesFor(t.dropoffLat, t.dropoffLng)
          boxes.foldLeft(w) { case (w2, b) => w2.addBox(b) }
      }
      .async
      .mergeSubstreams
      .mapConcat(_.close())
      .fold(List.empty[Hotspot]) { case (acc, hs) => hs :: acc }
      .runForeach { hotspots =>
        val sortedHotspots = hotspots.sortBy(_.count)
        sortedHotspots.foreach(println)
        println(s"Found ${hotspots.size} hotspots")
        HotspotJsFile.saveHotspotsAsJsData(hotspots)
      }

    import scala.concurrent.duration._

    try Await.result(f, 60.minutes)
    finally as.terminate()

    println(s"Finished in ${new Date().getTime - start.getTime} ms")
  }


}

trait WindowCommand {
  def wb: WindowBounds
}

case class OpenWindow(wb: WindowBounds) extends WindowCommand
case class CloseWindow(wb: WindowBounds) extends WindowCommand
case class AddToWindow(t: Trip, wb: WindowBounds) extends WindowCommand