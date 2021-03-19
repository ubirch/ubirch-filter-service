package com.ubirch.filter.util

import com.ubirch.filter.TestBase
import com.ubirch.filter.testUtils.MessageEnvelopeGenerator.generateMsgEnvelope
import com.ubirch.filter.util.ProtocolMessageUtils.{ msgPackConfig, rawPacket }

import java.io.ByteArrayOutputStream

class ProtocolMessageUtilsTest extends TestBase {

  "rawPacket" must {
    "throw NullPointerException if signed is null" in {
      val pm = generateMsgEnvelope().ubirchPacket
      pm.setSigned(null)
      assertThrows[NullPointerException](rawPacket(pm))
    }

    "not throw NullPointerException if signed is NOT null" in {
      val pm = generateMsgEnvelope().ubirchPacket
      val out = new ByteArrayOutputStream(255)
      val packer = msgPackConfig.newPacker(out)
      packer.writePayload(pm.getSigned)

      packer.packBinaryHeader(pm.getSignature.length)
      packer.writePayload(pm.getSignature)
      packer.flush()
      packer.close()

      val result = out.toByteArray
      rawPacket(pm).mustBe(result)
    }

  }

}
