package com.socrata.bq.soql.bqreps

import com.rojoma.json.v3.ast.{JNumber, JString, JNull, JValue}
import com.socrata.bq.soql.{BBQRep}
import com.socrata.soql.types.{SoQLNull, SoQLDouble, SoQLType, SoQLValue}

class DoubleRep extends BBQRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLDouble

  override val bigqueryType: String = "FLOAT"

  override def SoQL(row: Seq[String]): SoQLValue = {
    if (row.head == null) SoQLNull
    else SoQLDouble(row.head.toDouble)
  }

  override def jvalue(value: SoQLValue): JValue = {
    if (value == SoQLNull) JNull
    else {
      val soqlDouble = value.asInstanceOf[SoQLDouble].value
      if (soqlDouble.isInfinite || soqlDouble.isNaN) JString(soqlDouble.toString)
      else JNumber(soqlDouble)
    }
  }

  override def numColumns: Int = 1
}
