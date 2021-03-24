package com.ubirch.filter.model

sealed trait FilterReaction

sealed trait RejectUPP extends FilterReaction

object RejectCreateUPP extends RejectUPP

object RejectDisableUPP extends RejectUPP

object RejectEnableUPP extends RejectUPP

object RejectDeleteUPP extends RejectUPP

object ForwardUPP extends FilterReaction

object InvestigateFurther extends FilterReaction
