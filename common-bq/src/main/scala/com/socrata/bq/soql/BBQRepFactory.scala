package com.socrata.bq.soql

import com.socrata.bq.soql.bqreps._
import com.socrata.soql.types._
import com.typesafe.scalalogging.slf4j.Logging

object BBQRepFactory extends Logging {

  private val RepFactory = Map[SoQLType, BBQRep[SoQLType, SoQLValue]](
    SoQLVersion -> new VersionRep,
    SoQLID -> new IDRep,
    SoQLText -> new TextRep,
    SoQLNumber -> new NumberLikeRep(
      SoQLNumber(_),
      _.asInstanceOf[SoQLNumber].value
    ),
    SoQLMoney -> new NumberLikeRep(
      SoQLMoney(_),
      _.asInstanceOf[SoQLMoney].value
    ),
    SoQLFixedTimestamp -> new FixedTimestampRep,
    SoQLFloatingTimestamp -> new FloatingTimestampRep,
    SoQLDate -> new DateRep,
    SoQLDouble -> new DoubleRep,
    SoQLBoolean -> new BooleanRep,
    SoQLPoint -> new PointRep,
    SoQLMultiPolygon -> new MultiPolygonRep
  )

  def apply(givenType : SoQLType) : BBQRep[SoQLType, SoQLValue] = {
    RepFactory.getOrElse(givenType, new NullRep)
  }
}
