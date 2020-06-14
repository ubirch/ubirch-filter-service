package com.ubirch.filter.model.eventlog

case class LookupKeyRow(name: String, category: String, key: String, value: String)

object LookupKeyRow {
  def fromLookUpKey(lookupKey: LookupKey): Seq[LookupKeyRow] = {
    lookupKey.value.map(x => LookupKeyRow(lookupKey.name, lookupKey.category, lookupKey.key.name, x.name))
  }
}
