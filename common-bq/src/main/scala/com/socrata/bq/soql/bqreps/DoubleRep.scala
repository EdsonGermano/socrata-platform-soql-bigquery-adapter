package com.socrata.bq.soql.bqreps

import com.rojoma.json.v3.ast.{JNumber, JString, JNull, JValue}
import com.socrata.bq.soql.{BigQueryWriteRep, BigQueryReadRep}
import com.socrata.soql.types.{SoQLNull, SoQLDouble, SoQLType, SoQLValue}

class DoubleRep extends BigQueryReadRep[SoQLType, SoQLValue] with BigQueryWriteRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLDouble

  override val bigqueryType: String = "FLOAT"

  override def SoQL(row: Seq[String], index: Int): SoQLValue = {
    if (row(index) == null) SoQLNull
    else SoQLDouble(row(index).toDouble)
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
