package com.socrata.bq.soql.bqreps

import com.rojoma.json.v3.ast.{JNumber, JString, JNull, JValue}
import com.socrata.bq.soql.{BigQueryRep}
import com.socrata.soql.types.{SoQLNull, SoQLDouble, SoQLType, SoQLValue}

class DoubleRep extends BigQueryRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLDouble

  override val bigqueryType: String = "FLOAT"

  override def SoQL(value: String): SoQLValue = {
    if (value == null) SoQLNull
    else SoQLDouble(value.toDouble)
  }

  override def jvalue(value: SoQLValue): JValue = {
    if (value == SoQLNull) JNull
    else {
      val soqlDouble = value.asInstanceOf[SoQLDouble].value
      if (soqlDouble.isInfinite || soqlDouble.isNaN) JString(soqlDouble.toString)
      else JNumber(soqlDouble)
    }
  }
}
