package com.socrata.bq.query

import java.io.IOException

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.GenericJson
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.{BigqueryRequest, BigqueryScopes, Bigquery}
import com.google.api.services.bigquery.model._

/**
 * Queries Google BigQuery and returns data in pages to the client.
 * @param projectId The project id where the dataset and tables reside.
 */
class BBQQuerier(projectId: String) {

  // Wait time before checking on query status
  private val WAIT_TIME_BEFORE_POLLING : Long = 100

  // Whether or not batch queries should be executed. By default, batch queries
  // should not be executed since queries should be performed immediately after the are issued rather
  // than waiting around for idle resources.
  // More information here: https://cloud.google.com/bigquery/querying-data?hl=en#batchqueries
  private val BATCH : Boolean = false

  // BigQuery client
  private val BIG_QUERY = {
    var credential: GoogleCredential = GoogleCredential.getApplicationDefault
    if (credential.createScopedRequired) {
      credential = credential.createScoped(BigqueryScopes.all)
    }
    new Bigquery.Builder(new NetHttpTransport(), new JacksonFactory(), credential).setApplicationName("BBQ Querier").build()
  }

  /**
   * Issues a query to Bigquery. Returns an iterator with pages of data from Bigquery.
   * @param queryString Query to be executed on Bigquery.
   * @return An iterator to the result of the pages.
   * @throws IOException Thrown if there's a network exception
   * @throws InterruptedException Thrown if the query has been interrupted by Bigquery.
   */
  @throws(classOf[Exception])
  def query(queryString: String): Iterator[GetQueryResultsResponse] = {
    val query: Job = asyncQuery(BIG_QUERY, projectId, queryString)
    val getRequest: Bigquery#Jobs#Get = BIG_QUERY.jobs.get(projectId, query.getJobReference.getJobId)
    pollJob(getRequest, WAIT_TIME_BEFORE_POLLING)
    val resultsRequest: Bigquery#Jobs#GetQueryResults = BIG_QUERY.jobs.getQueryResults(projectId, query.getJobReference.getJobId)
    getPages(resultsRequest)
  }

  /**
   * Inserts an asynchronous query Job for a particular query.
   *
   * @param bigquery  an authorized BigQuery client
   * @param projectId a String containing the project ID
   * @param querySql  the actual query string
   * @return a reference to the inserted query job
   * @throws IOException Thrown if there's a network exception
   */
  @throws(classOf[IOException])
  private def asyncQuery(bigquery: Bigquery, projectId: String, querySql: String): Job = {
    val queryConfig: JobConfigurationQuery = new JobConfigurationQuery().setQuery(querySql)
    if (BATCH) {
      queryConfig.setPriority("BATCH")
    }
    val job: Job = new Job().setConfiguration(new JobConfiguration().setQuery(queryConfig))
    bigquery.jobs.insert(projectId, job).execute
  }

  /**
   * Polls the job for completion.
   * @param request The bigquery request to poll for completion
   * @param interval Number of milliseconds between each poll
   * @return The finished job
   * @throws IOException IOException
   * @throws InterruptedException InterruptedException
   */
  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  private def pollJob(request: Bigquery#Jobs#Get, interval: Long): Job = {
    var job: Job = request.execute
    while (!(job.getStatus.getState == "DONE")) {
      Thread.sleep(interval)
      job = request.execute
    }
    job
  }

  /**
   * Pages through the results of an arbitrary Bigquery request.
   * @param requestTemplate  The object that represents the call to fetch
   *                         the results.
   * @param <T> The type of the returned objects
   * @return An iterator that pages through the returned object
   */
  private def getPages[T <: GenericJson](requestTemplate: BigqueryRequest[T]): Iterator[T] = {
    class PageIterator extends Iterator[T] {
      private var request: BigqueryRequest[T] = null
      private var itrHasNext: Boolean = true

      /**
       * Inner class that represents our iterator to page through results.
       * @param requestTemplate The object that represents the call to fetch
       *                        the results.
       */
      def this(requestTemplate: BigqueryRequest[T]) {
        this()
        this.request = requestTemplate
      }

      /**
       * Checks whether there is another page of results.
       * @return True if there is another page of results.
       */
      def hasNext: Boolean = {
        itrHasNext
      }

      /**
       * Returns the next page of results.
       * @return The next page of results
       */
      def next(): T = {
        if (!hasNext) {
          throw new NoSuchElementException()
        }
        try {
          val response: T = request.execute
          if (response.containsKey("pageToken")) {
            request = request.set("pageToken", response.get("pageToken"))
          }
          else {
            itrHasNext = false
          }
          response
        }
      }

      /**
       * Skips the page by moving the iterator to the next page.
       */
      def remove() {
        this.next()
      }
    }
    new PageIterator(requestTemplate)
  }
}
