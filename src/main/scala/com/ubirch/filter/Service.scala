package com.ubirch.filter

object Service extends Boot(List(new Binder)){

  def main(args: Array[String]): Unit = {
    get[FilteringSystem].start()
  }

}
