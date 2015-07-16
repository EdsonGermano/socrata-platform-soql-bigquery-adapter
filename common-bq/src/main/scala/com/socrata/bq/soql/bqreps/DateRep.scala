package com.socrata.bq.soql.bqreps

import com.socrata.bq.soql.{SoQLBigQueryWriteRep, SoQLBigQueryReadRep}
import com.socrata.soql.types.{SoQLNull, SoQLDate, SoQLValue, SoQLType}
import org.joda.time.LocalDate

class DateRep extends SoQLBigQueryReadRep[SoQLType, SoQLValue] with SoQLBigQueryWriteRep[SoQLType, SoQLValue] {
  override def SoQL(value: String): SoQLValue = {
    if (value == null) SoQLNull
    else SoQLDate(LocalDate.parse(value))
  }

  override def BigQueryType(soqlType: SoQLType): String = "TIMESTAMP"

  override def repType: SoQLType = SoQLDate
}
