package com.socrata.bq.soql.bqreps

import com.rojoma.json.v3.ast.{JString, JNull, JValue}
import com.socrata.bq.soql.{BigQueryWriteRep, BigQueryReadRep}
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLType, SoQLFixedTimestamp}
import org.joda.time.DateTime

class FixedTimestampRep extends BigQueryReadRep[SoQLType, SoQLValue] with BigQueryWriteRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLFixedTimestamp

  override val bigqueryType: String = "TIMESTAMP"

  // TODO: this requires that the TIMESTAMP is extracted from BigQuery using TIMESTAMP_TO_USEC().
  override def SoQL(value: String): SoQLValue = {
    if (value == null) SoQLNull
    else SoQLFixedTimestamp(new DateTime(value.toDouble.toLong / 1000))
  }

  override def jvalue(value: SoQLValue): JValue = {
    if (value == SoQLNull) JNull
    else JString(SoQLFixedTimestamp.StringRep(value.asInstanceOf[SoQLFixedTimestamp].value))
  }
}
