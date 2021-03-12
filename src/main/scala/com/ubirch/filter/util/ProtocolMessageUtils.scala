package com.ubirch.filter.util

import com.ubirch.protocol.ProtocolMessage
import com.ubirch.protocol.codec.MsgPackProtocolDecoder
import org.msgpack.core.MessagePack

import java.io.ByteArrayOutputStream
import java.util.Base64

object ProtocolMessageUtils {

  val base64Decoder: Base64.Decoder = Base64.getDecoder
  val base64Encoder: Base64.Encoder = Base64.getEncoder
  val msgPackDecoder: MsgPackProtocolDecoder = MsgPackProtocolDecoder.getDecoder
  val msgPackConfig: MessagePack.PackerConfig = new MessagePack.PackerConfig().withStr8FormatSupport(false)

  /**
    * Method to make a short byte array of the message pack, which is done
    * the most easily by attaching the signed content and it's signature to
    * each other.
    *
    * @param pm The protocol message representation of the original msgPack.
    *           Attention: at this point in our pipeline it should contain
    *           signed.
    */
  def rawPacket(pm: ProtocolMessage): Array[Byte] = {
    val out = new ByteArrayOutputStream(255)
    val packer = msgPackConfig.newPacker(out)

    if (pm.getSigned != null) {
      packer.writePayload(pm.getSigned)
    } else {
      //this should never be the case, when the UPP has been processed by the other services correctly before
      throw new NullPointerException("signed was empty ")
    }

    packer.packBinaryHeader(pm.getSignature.length)
    packer.writePayload(pm.getSignature)
    packer.flush()
    packer.close()

    out.toByteArray
  }

}
