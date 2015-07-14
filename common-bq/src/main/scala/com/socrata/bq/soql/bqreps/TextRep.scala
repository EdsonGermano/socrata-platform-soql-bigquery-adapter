package com.socrata.bq.soql.bqreps

import com.socrata.bq.soql.SoQLBigQueryReadRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLText, SoQLType}

import scala.collection.mutable

class TextRep(val base : String) extends SoQLBigQueryReadRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLText

  override def toSoQL(value : String): SoQLValue = {
    if (value == null) SoQLNull
    else SoQLText(value)
  }

}
