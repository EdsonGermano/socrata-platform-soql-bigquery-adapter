package com.socrata.bq.query

import java.io.IOException

import com.google.api.services.bigquery.Bigquery
import com.google.api.services.bigquery.model._

import scala.collection.JavaConversions._


import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class BigQueryQuerier(projectId: String) {

  @throws(classOf[Exception])
  def query(queryString: String): ArrayBuffer[mutable.Buffer[String]] with TotalRowCount = {
    val batch: Boolean = false
    val waitTime: Long = 100
    val curTime: Long = System.currentTimeMillis

    val allPages: Array[GetQueryResultsResponse] = run(projectId, queryString, batch, waitTime).toArray

    val result = new ArrayBuffer[mutable.Buffer[String]]() with TotalRowCount

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
              row.update(i-1, s"${row.last},$m")
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

  /**
   *
   * @param projectId Get this from Google Developers console
   * @param queryString Query we want to run against BigQuery
   * @param batch True if you want to batch the queries
   * @param waitTime How long to wait before retries
   * @return An interator to the result of your pages
   * @throws IOException Thrown if there's an IOException
   * @throws InterruptedException Thrown if there's an Interrupted Exception
   */
  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  def run(projectId: String, queryString: String, batch: Boolean, waitTime: Long): Iterator[GetQueryResultsResponse] = {
    val bigquery: Bigquery = BigqueryServiceFactory.getService
    val query: Job = asyncQuery(bigquery, projectId, queryString, batch)
    val getRequest: Bigquery#Jobs#Get = bigquery.jobs.get(projectId, query.getJobReference.getJobId)
    BigqueryUtils.pollJob(getRequest, waitTime)
    val resultsRequest: Bigquery#Jobs#GetQueryResults = bigquery.jobs.getQueryResults(projectId, query.getJobReference.getJobId)
    BigqueryUtils.getPages(resultsRequest)
  }

  /**
   * Inserts an asynchronous query Job for a particular query.
   *
   * @param bigquery  an authorized BigQuery client
   * @param projectId a String containing the project ID
   * @param querySql  the actual query string
   * @param batch True if you want to run the query as BATCH
   * @return a reference to the inserted query job
   * @throws IOException Thrown if there's a network exception
   */
  @throws(classOf[IOException])
  def asyncQuery(bigquery: Bigquery, projectId: String, querySql: String, batch: Boolean): Job = {
    val queryConfig: JobConfigurationQuery = new JobConfigurationQuery().setQuery(querySql)
    if (batch) {
      queryConfig.setPriority("BATCH")
    }
    val job: Job = new Job().setConfiguration(new JobConfiguration().setQuery(queryConfig))
    bigquery.jobs.insert(projectId, job).execute
  }
}

