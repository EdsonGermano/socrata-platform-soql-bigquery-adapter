package com.socrata.bq.server

import com.socrata.bq.query.TotalRowCount

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Tests group
 */
class SoQLGroupQueryTest extends QueryTest {

  test("group by boolean") {
    val expected = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer("51", "false"), mutable.Buffer("49", "true"))
    queryAndCompare(s"SELECT COUNT(*), boolean FROM ${QueryTest.FULL_TABLE_NAME} GROUP BY boolean", expected, 2)
  }

  test("group by text order by count") {
    val group = ArrayBuffer[mutable.Buffer[String]](mutable.Buffer(""))
  }

}
