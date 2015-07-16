package com.socrata.bq.soql.bqreps

import com.socrata.bq.soql.{SoQLBigQueryWriteRep, SoQLBigQueryReadRep}
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLType, SoQLFixedTimestamp}
import org.joda.time.DateTime

class FixedTimestampRep extends SoQLBigQueryReadRep[SoQLType, SoQLValue] with SoQLBigQueryWriteRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLFixedTimestamp

  // TODO: this requires that the TIMESTAMP is extracted from BigQuery using TIMESTAMP_TO_USEC().
  override def SoQL(value: String): SoQLValue = {
    if (value == null) SoQLNull
    else SoQLFixedTimestamp(new DateTime(value.toDouble.toLong / 1000))
  }

  override def BigQueryType(soqlType: SoQLType): String = "TIMESTAMP"
}
