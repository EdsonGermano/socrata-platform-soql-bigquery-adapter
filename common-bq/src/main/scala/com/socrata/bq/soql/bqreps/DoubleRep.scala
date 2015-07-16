package com.socrata.bq.soql.bqreps

import com.socrata.bq.soql.{SoQLBigQueryWriteRep, SoQLBigQueryReadRep}
import com.socrata.soql.types.{SoQLNull, SoQLDouble, SoQLType, SoQLValue}

class DoubleRep extends SoQLBigQueryReadRep[SoQLType, SoQLValue] with SoQLBigQueryWriteRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLDouble

  override def SoQL(value: String): SoQLValue = {
    if (value == null) SoQLNull
    else SoQLDouble(value.toDouble)
  }

  override def BigQueryType(soqlType: SoQLType): String = "FLOAT"
}
