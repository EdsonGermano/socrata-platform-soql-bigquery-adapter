package com.socrata.bq.soql.bqreps

import com.rojoma.json.v3.ast.{JNumber, JString, JNull, JValue}
import com.socrata.bq.soql.{BigqueryType, BBQRep}
import com.socrata.soql.types.{SoQLNull, SoQLDouble, SoQLType, SoQLValue}

class DoubleRep extends BBQRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLDouble

  override val bigqueryType = BigqueryType.Float

  override def SoQL(cols: Seq[String]): SoQLValue = {
    if (cols.head == null) SoQLNull
    else SoQLDouble(cols.head.toDouble)
  }

  override def jvalue(value: SoQLValue): JValue = {
    if (value == SoQLNull) JNull
    else {
      val soqlDouble = value.asInstanceOf[SoQLDouble].value
      if (soqlDouble.isInfinite || soqlDouble.isNaN) JString(soqlDouble.toString)
      else JNumber(soqlDouble)
    }
  }

  override val numColumns: Int = 1
}
