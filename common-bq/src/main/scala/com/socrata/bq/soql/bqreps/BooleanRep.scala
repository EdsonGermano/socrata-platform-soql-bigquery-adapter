package com.socrata.bq.soql.bqreps

import com.socrata.bq.soql.{SoQLBigQueryWriteRep, SoQLBigQueryReadRep}
import com.socrata.soql.types.{SoQLNull, SoQLBoolean, SoQLType, SoQLValue}

class BooleanRep extends SoQLBigQueryReadRep[SoQLType, SoQLValue] with SoQLBigQueryWriteRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLBoolean

  override def SoQL(value: String): SoQLValue = {
    if (value == null) SoQLNull
    else if (value == "true") SoQLBoolean(true)
    else SoQLBoolean(false)
  }

  override def BigQueryType(soqlType: SoQLType): String = "BOOLEAN"
}
