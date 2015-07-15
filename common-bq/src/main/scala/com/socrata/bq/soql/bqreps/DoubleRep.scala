package com.socrata.bq.soql.bqreps

import com.socrata.bq.soql.SoQLBigQueryReadRep
import com.socrata.soql.types.{SoQLNull, SoQLDouble, SoQLType, SoQLValue}

class DoubleRep(val base : String) extends SoQLBigQueryReadRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLDouble

  override def toSoQL(value: String): SoQLValue = {
    if (value == null) SoQLNull
    else SoQLDouble(value.toDouble)
  }
}
