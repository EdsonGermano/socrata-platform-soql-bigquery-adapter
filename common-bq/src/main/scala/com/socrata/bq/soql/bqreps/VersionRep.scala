package com.socrata.bq.soql.bqreps

import com.rojoma.json.v3.ast.{JString, JNull, JValue}
import com.socrata.bq.soql.{BigqueryType, BBQRep}
import com.socrata.soql.types.{SoQLVersion, SoQLType, SoQLValue}

class VersionRep extends BBQRep[SoQLType, SoQLValue] {

  override def repType: SoQLType = SoQLVersion

  override val bigqueryType = BigqueryType.Integer

  override def SoQL(cols: Seq[String]): SoQLValue = {
    // should never be null
    SoQLVersion(cols.head.toLong)
  }

  override def jvalue(value: SoQLValue): JValue = {
    if (value == null) JNull
    else JString(value.asInstanceOf[SoQLVersion].value.toString)
  }

  override val numColumns: Int = 1
}
