package com.softwaremill.its

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{FileIO, Framing, Source}
import akka.util.ByteString

import scala.concurrent.Await

object AkkaHotspotDetector {
  def main(args: Array[String]): Unit = Timed {

    implicit val as = ActorSystem()
    implicit val mat = ActorMaterializer()

    HotspotJsFile.resetHotspotFile()

    val f = FileIO.fromPath(Paths.get(Config.csvFileName))
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024))
      .map(_.utf8String)
      .mapConcat(Trip.parseOrDiscard)
      .zip(Source.unfold(0)(st => Some((st + 1, st + 1))))
      .map { case (t, i) =>
        if (i % 1000 == 0) println(s"Processing trip $i")
        t
      }
      .fold(HotspotState(Nil, Nil)) { case (st, t) => st.addTrip(t) }
      .map(_.result())
      .runForeach { hotspots =>
        val sortedHotspots = hotspots.sortBy(_.count)
        sortedHotspots.foreach(println)
        println(s"Found ${hotspots.size} hotspots")
        HotspotJsFile.saveHotspotsAsJsData(hotspots)
      }

    import scala.concurrent.duration._

    try Await.result(f, 60.minutes)
    finally as.terminate()
  }
}

