package com.socrata.bq.soql.bqreps

import com.rojoma.json.v3.ast.{JString, JNull, JValue}
import com.socrata.bq.soql.{BigQueryRep}
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLType, SoQLFixedTimestamp}
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

class FixedTimestampRep extends BigQueryRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLFixedTimestamp

  override val bigqueryType: String = "TIMESTAMP"

  // TODO: this requires that the TIMESTAMP is extracted from BigQuery using TIMESTAMP_TO_USEC().
  override def SoQL(row: Seq[String], index: Int): SoQLValue = {
    if (row(index) == null) SoQLNull
    else SoQLFixedTimestamp(new DateTime(row(index).toDouble.toLong / 1000))
//    else SoQLFixedTimestamp(ISODateTimeFormat.dateTimeParser.withZoneUTC.parseDateTime(value))
  }

  override def jvalue(value: SoQLValue): JValue = {
    if (value == SoQLNull) JNull
    else JString(SoQLFixedTimestamp.StringRep(value.asInstanceOf[SoQLFixedTimestamp].value))
  }

  override def numColumns: Int = 1
}
