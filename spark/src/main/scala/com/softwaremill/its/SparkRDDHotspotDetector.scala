package com.softwaremill.its

import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDHotspotDetector {
  val aggregateProgress = new ProgressPrinter("Aggregate", 1000)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkRDDHotspotDetector").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val tripToBound = sc.longAccumulator("Trip to bound")
    val tripToWindow = sc.longAccumulator("Trip to window")

    sc
      .textFile(Config.csvFileName)
      .flatMap { l =>
        Trip.parseOrDiscard(l).flatMap { t =>
          tripToBound.add(1)
          WindowBounds.boundsFor(t.dropoffTime).map(_ -> t)
        }
      }
      .aggregateByKey(Window.Empty)(
        (w, t) => {
          aggregateProgress.inc()
          tripToWindow.add(1)
          w.addTrip(t)
        },
        _.addWindow(_)
      )
      .flatMap { case (wb, w) => w.copy(bounds = wb).close() }
      .collect()
      .foreach(println)
  }
}
