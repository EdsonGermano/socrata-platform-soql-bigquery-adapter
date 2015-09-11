package com.socrata.bq.server

import com.google.api.services.bigquery.model.GetQueryResultsResponse
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
import com.socrata.bq.query.{TotalRowCount, BBQQuerier, BQQueryTestBase}

import scala.collection.JavaConversions._


object QueryTest {
  // should not be hard coded
  val DATASET_ID = "test"
  val TABLE_ID = "dataset"
  val FULL_TABLE_NAME = s"[${DATASET_ID}.${TABLE_ID}]"
}

class QueryTest extends FunSuite with BeforeAndAfterAll with Matchers {

  var bqQuerier: BBQQuerier = null

  // TODO: Make the test database come from a conf. file
  override def beforeAll() = {
    bqQuerier = new BBQQuerier("thematic-bee-98521")
  }

  def queryAndCompare(queryString: String, expected: Option[ArrayBuffer[mutable.Buffer[String]]], rc: Integer) = {
    val result = processResult(bqQuerier.query(queryString))
    // wrap it in an option so that if the result is too big to compare, just compare the row count
    expected match {
      case Some(x) => result shouldEqual x
      case _ =>
    }
    result.rowCount shouldEqual rc
  }

  def processResult(res: Iterator[GetQueryResultsResponse]): ArrayBuffer[mutable.Buffer[String]] with TotalRowCount = {
        val result = new ArrayBuffer[mutable.Buffer[String]]() with TotalRowCount

        val allPages = res.toArray

        if (!allPages.isEmpty && allPages.head.getTotalRows.longValue > 0) {
          // Because BigQuery's API sucks, null values in the table are represented as java.lang.Object
          // objects. If it is of type String, then there is a value present in that TableCell, otherwise,
          // there is no object.

          // Iterate through the schema to see if the user requested a point
          val schema = allPages.head.getSchema.getFields

          allPages.map(x => x.getRows.map(
            r => {
              val row = new mutable.ArrayBuffer[String]()
              var i = 0
              r.getF.map(f => f.getV.getClass.getSimpleName match {
                case "String" => f.getV.toString
                case _ => null
              }).foreach(m => {
                if (schema(i).getName.endsWith("_long")) {
                  // This will throw an ArrayIndexOutOfBounds exception if longitude is ever at index 0
                  // We assume that latitude and longitude are selected together, as (lat, long)
                  row.update(i-1, s"$m,${row.last}")
                }
                else row.add(m)
                i = i + 1
              })
              result.add(row)
            }
          ))
        }
        result.rowCount = allPages.head.getTotalRows.longValue
        result
  }

}
