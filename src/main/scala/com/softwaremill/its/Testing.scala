package com.softwaremill.its

import java.nio.file.Paths
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, OffsetDateTime, ZoneOffset}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{FileIO, Framing, Sink, Source}
import akka.util.ByteString

import scala.concurrent.Await
import scala.util.Try
import scala.concurrent.duration._

object Testing extends App {
  implicit val as = ActorSystem()
  implicit val mat = ActorMaterializer()

  val f = FileIO.fromPath(Paths.get("/Users/adamw/projects/its/sorted_green_tripdata_2015-12.csv"))
    .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024))
    .map(_.utf8String)
    .map(_.split(","))
    .map(Trip.parse)
    .map(_.toOption)
    .collect { case Some(x) => x }
    .filter(_.isValid)
    .take(10)
    .to(Sink.foreach(println))
    .run()

  Await.result(f, 1.minute)
}

case class Trip(pickupLat: Double, pickupLng: Double, dropoffLat: Double, dropoffLng: Double,
  distance: Double, dropoffTime: OffsetDateTime) {

  def isValid = isInNy(pickupLat, pickupLng) && isInNy(dropoffLat, dropoffLng) && distance > 0.1
  def isInNy(lat: Double, lng: Double) = lat > 39.0 && lat < 43.0 && lng > -75.0 && lng < 71.0
}

object Trip {
  private val DateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  def parse(in: Array[String]) = Try {
    Trip(in(6).toDouble, in(5).toDouble, in(8).toDouble, in(7).toDouble, in(10).toDouble,
      LocalDateTime.parse(in(2), DateFormat).atOffset(ZoneOffset.of("-05:00"))) // EST
  }
}