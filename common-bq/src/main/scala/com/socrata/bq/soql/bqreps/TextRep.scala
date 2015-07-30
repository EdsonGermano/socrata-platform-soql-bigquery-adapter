package com.socrata.bq.soql.bqreps

import com.rojoma.json.v3.ast.{JString, JNull, JValue}
import com.socrata.bq.soql.{BigQueryWriteRep, BigQueryReadRep}
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLText, SoQLType}

import scala.collection.mutable

class TextRep extends BigQueryReadRep[SoQLType, SoQLValue] with BigQueryWriteRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLText

  override val bigqueryType: String = "STRING"

  override def SoQL(value : String): SoQLValue = {
    if (value == null) SoQLNull
    else SoQLText(value)
  }

  override def jvalue(value: SoQLValue): JValue = {
    if (value == SoQLNull) JNull
    else JString(value.asInstanceOf[SoQLText].value)
  }

  override def numColumns(): Long = 1
}
