package com.softwaremill.its

import java.util.concurrent.atomic.AtomicLong

class ProgressPrinter(name: String, printEvery: Int) {
  private val counter = new AtomicLong(0)

  def inc(count: Int = 1): Unit = {
    val c = counter.addAndGet(count)
    if (c % printEvery == 0) println(name + " @: " + c)
  }
}
