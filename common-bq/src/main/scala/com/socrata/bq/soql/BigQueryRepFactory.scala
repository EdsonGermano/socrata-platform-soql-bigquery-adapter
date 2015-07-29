package com.socrata.bq.soql

import com.socrata.bq.soql.bqreps._
import com.socrata.soql.types._
import com.typesafe.scalalogging.slf4j.Logging

object BigQueryRepFactory extends Logging {

  private val RepFactory = Map[SoQLType, BigQueryReadRep[SoQLType, SoQLValue] with BigQueryWriteRep[SoQLType, SoQLValue]](
    SoQLVersion -> new VersionRep,
    SoQLID -> new IDRep,
    SoQLText -> new TextRep,
    SoQLNumber -> new NumberRep,
    SoQLFixedTimestamp -> new FixedTimestampRep,
    SoQLFloatingTimestamp -> new FloatingTimestampRep,
    SoQLDate -> new DateRep,    // Date may not be working correctly
    SoQLDouble -> new DoubleRep,
    SoQLBoolean -> new BooleanRep,
    SoQLPoint -> new PointRep
  )

  def apply(givenType : SoQLType) : BigQueryReadRep[SoQLType, SoQLValue] with BigQueryWriteRep[SoQLType, SoQLValue] = {
    RepFactory.getOrElse(givenType, new NullRep)
  }
}
