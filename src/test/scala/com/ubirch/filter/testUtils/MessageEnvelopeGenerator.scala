package com.ubirch.filter.testUtils

import com.ubirch.kafka.MessageEnvelope
import com.ubirch.protocol.ProtocolMessage
import com.ubirch.protocol.codec.{ MsgPackProtocolDecoder, MsgPackProtocolEncoder }
import org.json4s.JsonAST.{ JObject, JString }

import java.util.UUID

object MessageEnvelopeGenerator {

  private val signer = new ProtocolMessageSigner()
  private val msgPackEncoder = MsgPackProtocolEncoder.getEncoder

  /**
    * Method to generate a message envelope as expected by the filter service.
    *
    * @param payload Payload is a random value if not defined explicitly.
    * @return
    */
  def generateMsgEnvelope(
      uuid: UUID = UUID.randomUUID(),
      version: Int = 34,
      hint: Int = 0,
      payload: Object = "89319".getBytes()
  ): MessageEnvelope = {

    val pmNew = new ProtocolMessage(version, uuid, hint, payload)
    val pmRaw = msgPackEncoder.encode(pmNew, signer)
    val pm = MsgPackProtocolDecoder.getDecoder.decode(pmRaw)
    val context = JObject("customerId" -> JString(UUID.randomUUID().toString))
    MessageEnvelope(pm, context)
  }

}
