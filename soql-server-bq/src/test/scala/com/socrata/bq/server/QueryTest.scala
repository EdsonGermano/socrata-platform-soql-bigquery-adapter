package com.socrata.bq.server

import com.socrata.bq.server.config.QueryServerConfig
import com.socrata.datacoordinator.id.{RowId, UserColumnId}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.bq.store._
import com.socrata.soql.analyzer.SoQLAnalyzerHelper
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.environment.DatasetContext
import com.socrata.soql.environment.TypeName
import com.socrata.soql.types.{SoQLID, SoQLType}
import com.socrata.soql.collection.OrderedMap
import com.socrata.thirdparty.typesafeconfig.Propertizer
import com.socrata.soql.SoQLAnalysis
import java.sql.Connection
import com.typesafe.config.ConfigFactory
import org.apache.log4j.PropertyConfigurator
import org.scalatest.{Matchers, BeforeAndAfterAll, FunSuite}


import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.existentials
import com.socrata.bq.soql.{CaseSensitive}
import com.socrata.http.server.util.NoPrecondition
import com.socrata.bq.query.{TotalRowCount, BigQueryQuerier, BQQueryTestBase}

class QueryTest extends FunSuite with BeforeAndAfterAll with Matchers {

  var bqQuerier: BigQueryQuerier = null

  // TODO: Make the test database come from a conf. file

  override def beforeAll() = {
    bqQuerier = new BigQueryQuerier("thematic-bee-98521")
  }

  def queryAndCompare(queryString: String, expected: ArrayBuffer[mutable.Buffer[String]] with TotalRowCount) = {
    val result = bqQuerier.query(queryString)
    result shouldEqual expected
    result.rowCount shouldEqual expected.rowCount
  }

}
