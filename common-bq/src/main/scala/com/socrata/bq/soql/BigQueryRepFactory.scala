package com.socrata.bq.soql

import com.socrata.bq.soql.bqreps._
import com.socrata.soql.types._
import com.typesafe.scalalogging.slf4j.Logging

object BigQueryRepFactory extends Logging {

  private val RepFactory = Map[SoQLType, BigQueryRep[SoQLType, SoQLValue]](
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
    SoQLDate -> new DateRep,    // Date may not be working correctly
    SoQLDouble -> new DoubleRep,
    SoQLBoolean -> new BooleanRep,
    SoQLPoint -> new PointRep,
    SoQLMultiPolygon -> new MultiPolygonRep
  )

  def apply(givenType : SoQLType) : BigQueryRep[SoQLType, SoQLValue] = {
    RepFactory.getOrElse(givenType, new NullRep)
  }
}
