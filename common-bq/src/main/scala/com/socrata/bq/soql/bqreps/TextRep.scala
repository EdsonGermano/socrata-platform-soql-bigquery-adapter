package com.socrata.bq.soql.bqreps

import com.rojoma.json.v3.ast.{JString, JNull, JValue}
import com.socrata.bq.soql.{BigqueryType, BBQRep}
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLText, SoQLType}

import scala.collection.mutable

class TextRep extends BBQRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLText

  override val bigqueryType = BigqueryType.String

  override def SoQL(cols: Seq[String]): SoQLValue = {
    if (cols.head == null) SoQLNull
    else SoQLText(cols.head)
  }

  override def jvalue(value: SoQLValue): JValue = {
    if (value == SoQLNull) JNull
    else JString(value.asInstanceOf[SoQLText].value)
  }

  override val numColumns: Int = 1
}
