package com.ubirch.filter.util

import com.ubirch.kafka.MessageEnvelope
import com.ubirch.protocol.ProtocolMessage
import com.ubirch.protocol.codec.{MsgPackProtocolDecoder, MsgPackProtocolEncoder}
import org.json4s.JsonAST.{JObject, JString}
import org.msgpack.core.MessagePack

import java.io.ByteArrayOutputStream
import java.util.{Base64, UUID}

object MessageEnvelopeGenerator {

  private val signer = new ProtocolMessageSigner()
  private val msgPackEncoder = MsgPackProtocolEncoder.getEncoder
  private val msgPackConfig = new MessagePack.PackerConfig().withStr8FormatSupport(false)

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

  /**
    * Method to make a short byte array of the message pack, which is done
    * the most easily by attaching the signed content and it's signature to
    * each other.
    *
    * @param upp The protocol message representation of the original msgPack.
    *            Attention: at this point in our pipeline it should contain
    *            signed.
    */
  def rawPacket(upp: ProtocolMessage): Array[Byte] = {
    val out = new ByteArrayOutputStream(255)
    val packer = msgPackConfig.newPacker(out)
    //Todo: Error handling if signed doesn't exists
    packer.writePayload(upp.getSigned)
    packer.packBinaryHeader(upp.getSignature.length)
    packer.writePayload(upp.getSignature)
    packer.flush()
    packer.close()

    out.toByteArray
  }

  def b64(x: Array[Byte]): String = if (x != null) Base64.getEncoder.encodeToString(x) else null
}
