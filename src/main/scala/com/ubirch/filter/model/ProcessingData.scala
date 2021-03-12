package com.ubirch.filter.model

import com.ubirch.protocol.ProtocolMessage
import org.apache.kafka.clients.consumer.ConsumerRecord

import java.nio.charset.StandardCharsets

case class ProcessingData(cr: ConsumerRecord[String, String], pm: ProtocolMessage) {
  def payloadHash: Array[Byte] = pm.getPayload.asText().getBytes(StandardCharsets.UTF_8)

  def payloadString: String = pm.getPayload.asText()
}

