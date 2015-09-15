package com.socrata.bq.soql.bqreps

import com.rojoma.json.v3.ast._
import com.socrata.bq.soql.BBQRep
import com.socrata.soql.types.{SoQLNull, SoQLNumber, SoQLValue, SoQLType}

class NumberLikeRep(encode: java.math.BigDecimal => SoQLValue,
                    decode: SoQLValue => java.math.BigDecimal)
  extends BBQRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLNumber

  override val bigqueryType: String = "FLOAT"

  override def SoQL(row: Seq[String]): SoQLValue = {
    if (row.head == null) SoQLNull
    else encode(new java.math.BigDecimal(row.head))
  }

  override def jvalue(value: SoQLValue): JValue = {
    if (value == SoQLNull) JNull
    else JNumber(decode(value))
  }

  override val numColumns: Int = 1
}
