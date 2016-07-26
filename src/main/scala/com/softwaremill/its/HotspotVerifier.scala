package com.softwaremill.its

import java.nio.file.Paths
import java.time.format.DateTimeFormatter
import java.time._

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{FileIO, Framing, Source}
import akka.util.ByteString

import scala.concurrent.Await
import scala.util.Try

object HotspotVerifier extends App {
  implicit val as = ActorSystem()
  implicit val mat = ActorMaterializer()

  val toVerifyBounds = WindowBounds(OffsetDateTime.parse("2015-12-08T08:40-05:00"), OffsetDateTime.parse("2015-12-08T09:10-05:00"))
  val toVerifyPos = (40.809150, -73.961400)

  import GridBox._

  val f = FileIO.fromPath(Paths.get("/Users/adamw/projects/its/sorted_green_tripdata_2015-12.csv"))
    .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024))
    .map(_.utf8String)
    .map(_.split(","))
    .map(Trip.parseGreen)
    .map(_.toOption)
    .collect { case Some(x) => x }
    .filter { t => toVerifyBounds.contains(t.dropoffTime) }
    .filter { t =>
      t.dropoffLat >= toVerifyPos._1 - BoxUnits/2.0d*LatUnit &&
        t.dropoffLat <  toVerifyPos._1 + BoxUnits/2.0d*LatUnit &&
        t.dropoffLng >= toVerifyPos._2 - BoxUnits/2.0d*LngUnit &&
        t.dropoffLng <  toVerifyPos._2 + BoxUnits/2.0d*LngUnit
    }.map { t =>
    println(t)
    t
  }
    .runFold(0) { case (c, _) => c + 1 }

  import scala.concurrent.duration._

  try println("Total: " + Await.result(f, 10.minutes))
  finally as.terminate()
}
