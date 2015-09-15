package com.socrata.bq.query

import java.io.IOException

import com.google.api.services.bigquery.Bigquery
import com.google.api.services.bigquery.model._
import scala.collection.JavaConversions._

class BBQQuerier(projectId: String) {

  @throws(classOf[Exception])
  def query(queryString: String): Iterator[GetQueryResultsResponse] = {
    val batch: Boolean = false
    val waitTime: Long = 100
    val curTime: Long = System.currentTimeMillis

    run(projectId, queryString, batch, waitTime)
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
    val bigquery: Bigquery = BBQServiceFactory.getService
    val query: Job = asyncQuery(bigquery, projectId, queryString, batch)
    val getRequest: Bigquery#Jobs#Get = bigquery.jobs.get(projectId, query.getJobReference.getJobId)
    BBQUtils.pollJob(getRequest, waitTime)
    val resultsRequest: Bigquery#Jobs#GetQueryResults = bigquery.jobs.getQueryResults(projectId, query.getJobReference.getJobId)
    BBQUtils.getPages(resultsRequest)
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

