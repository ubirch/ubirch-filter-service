package com.ubirch.filter.model

object Values {

  val PAYLOAD = "payload"
  val UPP_CATEGORY = "UPP"
  val PRODUCTION_NAME = "prod"
  val PREVIOUS_MICROSERVICE = "previous-microservice"
  val HTTP_STATUS_CODE_HEADER = "http-status-code"
  val HTTP_STATUS_CODE_REJECTION_ERROR = "409"
  val UBIRCH_ERROR_CODE_HEADER = "x-code"
  val UBIRCH_ERROR_CODE_CREATE = "0000"
  val UBIRCH_ERROR_CODE_DISABLE = "0010"
  val UBIRCH_ERROR_CODE_ENABLE = "0020"
  val UBIRCH_ERROR_CODE_DELETE = "0030"
  val FOUND_IN_VERIFICATION_MESSAGE = "the hash/payload has been found by the verification lookup."
  val NOT_FOUND_IN_VERIFICATION_MESSAGE = "the hash/payload has not been found by the verification lookup; an update is not possible."
  val REPLAY_ATTACK_NAME = "replay_attack"
  val FOUND_IN_CACHE_MESSAGE = "the hash/payload has been found in the cache."

  val UPP_TYPE_DISABLE = 250
  val UPP_TYPE_ENABLE = 251
  val UPP_TYPE_DELETE = 252
}
