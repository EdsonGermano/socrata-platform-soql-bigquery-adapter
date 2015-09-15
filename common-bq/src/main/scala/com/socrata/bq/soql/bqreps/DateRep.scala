package com.socrata.bq.soql.bqreps

import com.rojoma.json.v3.ast.JValue
import com.socrata.bq.soql.BBQRep
import com.socrata.soql.types.{SoQLNull, SoQLDate, SoQLValue, SoQLType}
import org.joda.time.LocalDate

class DateRep extends BBQRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLDate

  override val bigqueryType: String = "TIMESTAMP"

  override def SoQL(cols: Seq[String]): SoQLValue = {
    if (cols.head == null) SoQLNull
    else SoQLDate(new LocalDate(cols.head.toDouble.toLong))
  }

  override def jvalue(value: SoQLValue): JValue = ???

  override val numColumns: Int = 1
}
