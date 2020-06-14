package com.ubirch.filter.util

import com.ubirch.filter.model.eventlog.CustomSerializers
import com.ubirch.util.JsonHelper

object EventLogJsonSupport extends JsonHelper(CustomSerializers.all)

