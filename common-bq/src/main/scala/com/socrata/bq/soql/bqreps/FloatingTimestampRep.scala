package com.socrata.bq.soql.bqreps

import com.rojoma.json.v3.ast.{JString, JNull, JValue}
import com.socrata.bq.soql.{BigqueryType, BBQRep}
import com.socrata.soql.types.{SoQLNull, SoQLFloatingTimestamp, SoQLType, SoQLValue}
import org.joda.time.{DateTimeZone, LocalDateTime}

class FloatingTimestampRep extends BBQRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLFloatingTimestamp

  override val bigqueryType = BigqueryType.Timestamp

  override def SoQL(cols: Seq[String]): SoQLValue = {
    if (cols.head== null)
      SoQLNull
    else
      // Timestamp strings can be returned from BQ as either plain numbers or scientific notation,
      // so we need the additional toDouble conversion to avoid NumberFormatExceptions
      SoQLFloatingTimestamp(new LocalDateTime(cols.head.toDouble.toLong, DateTimeZone.UTC))
  }

  override def jvalue(value: SoQLValue): JValue = {
    if (value == SoQLNull) JNull
    else JString(SoQLFloatingTimestamp.StringRep(value.asInstanceOf[SoQLFloatingTimestamp].value))
  }

  override val numColumns: Int = 1
}
