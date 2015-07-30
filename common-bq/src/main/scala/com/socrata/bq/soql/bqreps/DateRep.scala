package com.socrata.bq.soql.bqreps

import com.rojoma.json.v3.ast.JValue
import com.socrata.bq.soql.{BigQueryWriteRep, BigQueryReadRep}
import com.socrata.soql.types.{SoQLNull, SoQLDate, SoQLValue, SoQLType}
import org.joda.time.LocalDate

// Broken for now
class DateRep extends BigQueryReadRep[SoQLType, SoQLValue] with BigQueryWriteRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLDate

  override val bigqueryType: String = "TIMESTAMP"

  override def SoQL(value: String): SoQLValue = {
    if (value == null) SoQLNull
    else SoQLDate(new LocalDate(value.toDouble.toLong))
  }

  override def jvalue(value: SoQLValue): JValue = ???

  override def numColumns(): Long = 1
}
