package com.softwaremill.its

object Timed {
  def apply[T](b: => T): T = {
    val start = System.currentTimeMillis()
    val result = b
    println(s"Finished in ${System.currentTimeMillis() - start} ms")
    result
  }
}
