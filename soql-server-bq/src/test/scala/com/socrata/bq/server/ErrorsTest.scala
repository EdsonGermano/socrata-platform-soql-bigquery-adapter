package com.socrata.bq.server

import com.google.api.client.googleapis.json.GoogleJsonResponseException

class ErrorsTest extends QueryTest {

  test("invalid query string") {
    val thrown = intercept[GoogleJsonResponseException] {
      queryAndCompare(s"SELECT integer q FRO ${QueryTest.FULL_TABLE_NAME}", None, 0)
    }
    assert(thrown.getDetails.getCode === 400)
  }

  test("functions in order by") {
    val thrown = intercept[GoogleJsonResponseException] {
      queryAndCompare(s"SELECT integer FROM ${QueryTest.FULL_TABLE_NAME} ORDER BY floor(integer)", None, 0)
    }
    assert(thrown.getDetails.getCode === 400)
  }

  test("functions in group by") {
    val thrown = intercept[GoogleJsonResponseException] {
      queryAndCompare(s"SELECT floor(integer) FROM ${QueryTest.FULL_TABLE_NAME} GROUP BY f", None, 0)
    }
    assert(thrown.getDetails.getCode === 400)
  }

}
