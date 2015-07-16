package com.socrata.bq.soql.bqreps

import com.socrata.bq.soql.{SoQLBigQueryWriteRep, SoQLBigQueryReadRep}
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLText, SoQLType}

import scala.collection.mutable

class TextRep extends SoQLBigQueryReadRep[SoQLType, SoQLValue] with SoQLBigQueryWriteRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLText

  override def SoQL(value : String): SoQLValue = {
    if (value == null) SoQLNull
    else SoQLText(value)
  }

  override def BigQueryType(soqlType: SoQLType): String = "STRING"
}
