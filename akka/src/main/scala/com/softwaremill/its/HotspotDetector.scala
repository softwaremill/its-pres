package com.softwaremill.its

import java.nio.file.Paths
import java.util.Date

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{FileIO, Framing, Source}
import akka.util.ByteString

import scala.concurrent.Await

object HotspotDetector {
  def main(args: Array[String]): Unit = {

    implicit val as = ActorSystem()
    implicit val mat = ActorMaterializer()

    HotspotJsFile.resetHotspotFile()

    val start = new Date()

    val f = FileIO.fromPath(Paths.get(Config.csvFileName))
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024))
      .map(_.utf8String)
      .map(_.split(","))
      .map(a => if (Config.Green) Trip.parseGreen(a) else Trip.parseYellow(a))
      .map(_.toOption)
      .collect { case Some(x) => x }
      .filter(_.isValid)
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

    println(s"Finished in ${new Date().getTime - start.getTime} ms")
  }


}

