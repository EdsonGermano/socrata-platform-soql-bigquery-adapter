package com.socrata.bq.soql.bqreps

import com.socrata.bq.soql.SoQLBigQueryReadRep
import com.socrata.soql.types.{SoQLNull, SoQLNumber, SoQLValue, SoQLType}

class NumberLikeRep(conversion : java.math.BigDecimal => SoQLValue) extends SoQLBigQueryReadRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLNumber

  override def SoQL(value: String): SoQLValue = {
    if (value == null) SoQLNull
    else conversion(new java.math.BigDecimal(value.toDouble))
  }
}
