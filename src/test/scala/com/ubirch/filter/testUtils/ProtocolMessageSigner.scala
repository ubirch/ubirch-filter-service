package com.ubirch.filter.testUtils

import com.typesafe.scalalogging.StrictLogging
import com.ubirch.crypto.GeneratorKeyFactory
import com.ubirch.crypto.utils.Curve
import com.ubirch.protocol.Protocol
import org.apache.commons.codec.binary.Hex

import java.io.IOException
import java.security.{ InvalidKeyException, MessageDigest, NoSuchAlgorithmException, SignatureException }
import java.util
import java.util.UUID

class ProtocolMessageSigner() extends Protocol with StrictLogging {

  private val privKey = "a6abdc5466e0ab864285ba925452d02866638a8acb5ebdc065d2506661301417"
  private val pubKey = "b12a906051f102881bbb487ee8264aa05d8d0fcc51218f2a47f562ceb9b0d068"

  private val EdDSAKeyPrivatePart = Hex.decodeHex(privKey)
  private val EdDSAKeyPublicPart = Hex.decodeHex(pubKey)

  private val privateKey = GeneratorKeyFactory.getPrivKey(EdDSAKeyPrivatePart, Curve.Ed25519)
  private val publicKey = GeneratorKeyFactory.getPubKey(EdDSAKeyPublicPart, Curve.Ed25519)
  private val sha512 = MessageDigest.getInstance("SHA-512")
  private val zeroSignature = new Array[Byte](64)
  private val signatures = new util.HashMap[UUID, Array[Byte]]

  @throws[InvalidKeyException]
  @throws[SignatureException]
  override def sign(uuid: UUID, data: Array[Byte], offset: Int, len: Int): Array[Byte] =
    try {
      val md: MessageDigest = sha512.clone.asInstanceOf[MessageDigest]
      md.update(data, offset, len)
      val dataToSign = md.digest
      val signature = privateKey.sign(dataToSign)
      signatures.put(uuid, signature)
      logger.debug("HASH: ${dataToSign.length} ${Hex.encodeHexString(dataToSign)}")
      logger.debug("SIGN: ${signature.length}, ${Hex.encodeHexString(signature}")
      signature
    } catch {
      case e: CloneNotSupportedException =>
        logger.error("unable to clone SHA512 instance", e)
        null
    }

  @throws[SignatureException]
  override def verify(uuid: UUID, data: Array[Byte], offset: Int, len: Int, signature: Array[Byte]): Boolean =
    try {
      val md = sha512.clone.asInstanceOf[MessageDigest]
      md.update(data, offset, len)
      val dataToVerify = md.digest
      logger.debug("VRFY: ${ signature.length} $ Hex.encodeHexString(signature)")
      publicKey.verify(dataToVerify, signature)
    } catch {
      case e: NoSuchAlgorithmException =>
        logger.error("algorithm not found", e)
        false
      case e: InvalidKeyException =>
        logger.error("invalid key", e)
        false
      case e: CloneNotSupportedException =>
        logger.error("unable to clone SHA512 instance", e)
        false
      case e: SignatureException =>
        logger.warn("verification failed: m=Hex.encodeHexString(data) s=Hex.encodeHexString(signature)")
        throw new SignatureException(e)
      case e: IOException =>
        throw new SignatureException(e)
    }

  override protected def getLastSignature(uuid: UUID): Array[Byte] = signatures.getOrDefault(uuid, zeroSignature)
}
