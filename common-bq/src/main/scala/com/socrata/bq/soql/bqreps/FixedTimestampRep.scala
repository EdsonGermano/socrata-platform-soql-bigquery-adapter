package com.socrata.bq.soql.bqreps

import com.socrata.bq.soql.SoQLBigQueryReadRep
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLType, SoQLFixedTimestamp}
import org.joda.time.DateTime

class FixedTimestampRep extends SoQLBigQueryReadRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLFixedTimestamp

  override def SoQL(value: String): SoQLValue = {
    if (value == null) SoQLNull
    else SoQLFixedTimestamp(new DateTime(value.toDouble.toLong / 1000))
  }
}
