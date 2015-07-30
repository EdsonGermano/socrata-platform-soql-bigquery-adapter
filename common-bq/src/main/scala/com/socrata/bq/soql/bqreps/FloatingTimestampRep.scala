package com.socrata.bq.soql.bqreps

import com.rojoma.json.v3.ast.{JString, JNull, JValue}
import com.socrata.bq.soql.BigQueryRep
import com.socrata.soql.types.{SoQLNull, SoQLFloatingTimestamp, SoQLType, SoQLValue}
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTimeZone, LocalDateTime}

class FloatingTimestampRep extends BigQueryRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLFloatingTimestamp

  override val bigqueryType: String = "TIMESTAMP"

  override def SoQL(row: Seq[String]): SoQLValue = {
    if (row.head== null) SoQLNull
    else SoQLFloatingTimestamp(new LocalDateTime(row.head.toDouble.toLong / 1000, DateTimeZone.UTC))
//    else SoQLFloatingTimestamp(ISODateTimeFormat.localDateOptionalTimeParser.parseLocalDateTime(value))
  }

  override def jvalue(value: SoQLValue): JValue = {
    if (value == SoQLNull) JNull
    else JString(SoQLFloatingTimestamp.StringRep(value.asInstanceOf[SoQLFloatingTimestamp].value))
  }

  override def numColumns: Int = 1
}
