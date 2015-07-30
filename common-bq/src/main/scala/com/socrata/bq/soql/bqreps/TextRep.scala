package com.socrata.bq.soql.bqreps

import com.rojoma.json.v3.ast.{JString, JNull, JValue}
import com.socrata.bq.soql.{BigQueryRep}
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLText, SoQLType}

import scala.collection.mutable

class TextRep extends BigQueryRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLText

  override val bigqueryType: String = "STRING"

  override def SoQL(row: Seq[String], index: Int): SoQLValue = {
    if (row(index) == null) SoQLNull
    else SoQLText(row(index))
  }

  override def jvalue(value: SoQLValue): JValue = {
    if (value == SoQLNull) JNull
    else JString(value.asInstanceOf[SoQLText].value)
  }

  override def numColumns: Int = 1
}
