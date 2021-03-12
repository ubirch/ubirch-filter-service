package com.ubirch.filter.services.kafka

sealed trait FilterReaction

object RejectUPP extends FilterReaction

object ForwardUPP extends FilterReaction

object InvestigateFurther extends FilterReaction
