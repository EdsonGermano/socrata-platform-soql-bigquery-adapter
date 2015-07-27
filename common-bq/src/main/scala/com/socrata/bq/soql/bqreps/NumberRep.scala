package com.socrata.bq.soql.bqreps

import com.rojoma.json.v3.ast._
import com.socrata.bq.soql.{BigQueryWriteRep, BigQueryReadRep}
import com.socrata.soql.types.{SoQLNull, SoQLNumber, SoQLValue, SoQLType}

class NumberRep extends BigQueryReadRep[SoQLType, SoQLValue] with BigQueryWriteRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLNumber

  override val bigqueryType: String = "FLOAT"

  override def SoQL(value: String): SoQLValue = {
    if (value == null) SoQLNull
    else SoQLNumber(new java.math.BigDecimal(value.toDouble))
  }

  override def jvalue(value: SoQLValue): JValue = {
    if (value == SoQLNull) JNull
    else JNumber(value.asInstanceOf[SoQLNumber].value)
  }
}