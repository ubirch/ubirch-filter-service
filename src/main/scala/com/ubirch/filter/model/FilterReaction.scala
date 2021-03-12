package com.ubirch.filter.model

sealed trait FilterReaction

object RejectUPP extends FilterReaction

object ForwardUPP extends FilterReaction

object InvestigateFurther extends FilterReaction
