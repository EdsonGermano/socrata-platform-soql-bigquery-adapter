package com.socrata.bq.server

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SoQLGeometryQueriesTest extends QueryTest {

  test("test bounding box") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("-173.5919952392578", "23.348430633544922"),
      mutable.Buffer("-177.97438049316406", "42.689701080322266"))
    queryAndCompare(s"SELECT record.longitude, record.latitude FROM ${QueryTest.FULL_TABLE_NAME} WHERE " +
      s"record.longitude > -180.0 AND record.longitude < -170.0 AND record.latitude > 0.0 " +
      s"AND record.latitude < 60.0", Some(expected), 2)
  }

  test("test surround circle") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("-8.19264030456543", "-10.247159957885742"))
    queryAndCompare(s"SELECT record.latitude, record.longitude FROM ${QueryTest.FULL_TABLE_NAME} WHERE (((ACOS(SIN(0 * PI() / 180) * " +
      s"SIN((record.latitude/1000) * PI() / 180) + COS(0 * PI() / 180) * COS((record.latitude/1000) * PI() / 180) * " +
      s"COS(((0) - (record.longitude/1000)) * PI() / 180)) * 180 / PI()) * 60 * 1.1515) < 1)", Some(expected), 1)
  }

}
