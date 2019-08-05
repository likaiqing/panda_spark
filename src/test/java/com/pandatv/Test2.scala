package com.pandatv

object Test2 {
  def main(args: Array[String]): Unit = {
    val map = Map("a" -> "b", "c" -> "d")
    var map2 = Map("a" -> "b", "c" -> "d")
    map2 = map2.+("e" -> "d")
    map.+("e"->"f")
    println(map)
    println(map2)
  }

}
