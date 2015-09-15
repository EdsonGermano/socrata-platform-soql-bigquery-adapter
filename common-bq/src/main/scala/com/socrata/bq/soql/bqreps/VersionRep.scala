package com.socrata.bq.soql.bqreps

import com.rojoma.json.v3.ast.{JString, JNull, JValue}
import com.socrata.bq.soql.BBQRep
import com.socrata.soql.types.{SoQLVersion, SoQLType, SoQLValue}

class VersionRep extends BBQRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLVersion

  override val bigqueryType: String = "INTEGER"

  override def SoQL(row: Seq[String]): SoQLValue = {
    // should never be null
    SoQLVersion(row.head.toLong)
  }

  override def jvalue(value: SoQLValue): JValue = {
    if (value == null) JNull
    else JString(value.asInstanceOf[SoQLVersion].value.toString)
  }

  override val numColumns: Int = 1
}
