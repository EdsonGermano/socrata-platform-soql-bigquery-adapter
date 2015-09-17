package com.socrata.bq.soql

import com.socrata.bq.soql.bqreps._
import com.socrata.soql.types._
import com.typesafe.scalalogging.slf4j.Logging

/**
 * Retrieves the appropriate SoQL/BBQ conversion rep for the given SoQLType.
 * If a rep is not implemented for the given SoQLType, returns a NullRep.
 */
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
    SoQLDate -> new DateRep,  // Probably unused
    SoQLDouble -> new DoubleRep,
    SoQLBoolean -> new BooleanRep,
    SoQLPoint -> new PointRep,
    SoQLMultiPolygon -> new MultiPolygonRep
  )

  def apply(givenType : SoQLType): BBQRep[SoQLType, SoQLValue] = {
    RepFactory.getOrElse(givenType, new NullRep)
  }
}
