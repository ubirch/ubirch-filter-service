package com.ubirch.filter.util

import java.nio.charset.StandardCharsets

import com.ubirch.crypto.utils.Utils

object SigningHelper {

  def getBytesFromString(string: String): Array[Byte] = {
    string.getBytes(StandardCharsets.UTF_8)
  }

  def bytesToHex(bytes: Array[Byte]): String = {
    Utils.bytesToHex(bytes)
  }




}
