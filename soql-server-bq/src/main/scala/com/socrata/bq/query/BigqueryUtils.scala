package com.socrata.bq.query

import com.google.api.client.json.GenericJson
import com.google.api.services.bigquery.Bigquery
import com.google.api.services.bigquery.BigqueryRequest
import com.google.api.services.bigquery.model.DatasetList
import com.google.api.services.bigquery.model.Job
import com.google.api.services.bigquery.model.TableRow
import java.io.IOException
import java.io.PrintStream
import java.util.Iterator
import java.util.List
import java.util.NoSuchElementException

/**
 * Helper functions for the other classes.
 */
object BigqueryUtils {
  /**
   * Print rows to the output stream in a formatted way.
   * @param rows rows in bigquery
   * @param out Output stream we want to print to
   */
  def printRows(rows: List[TableRow], out: PrintStream) {
    import scala.collection.JavaConversions._
    for (row <- rows) {
      import scala.collection.JavaConversions._
      for (field <- row.getF) {
        out.printf("%-50s", field.getV)
      }
      out.println
    }
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
  def pollJob(request: Bigquery#Jobs#Get, interval: Long): Job = {
    var job: Job = request.execute
    while (!(job.getStatus.getState == "DONE")) {
      System.out.println("Job is " + job.getStatus.getState + " waiting " + interval + " milliseconds...")
      Thread.sleep(interval)
      job = request.execute
    }
    return job
  }

  /**
   * Pages through the results of an arbitrary Bigquery request.
   * @param requestTemplate  The object that represents the call to fetch
   *                         the results.
   * @param <T> The type of the returned objects
   * @return An iterator that pages through the returned object
   */
  def getPages[T <: GenericJson](requestTemplate: BigqueryRequest[T]): Iterator[T] = {
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
       * @return The next page of resul.ts
       */
      def next: T = {
        if (!hasNext) {
          throw new NoSuchElementException
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
      def remove {
        this.next
      }
    }
    return new PageIterator(requestTemplate)
  }

  /**
   * Display all BigQuery datasets associated with a project.
   *
   * @param bigquery  an authorized BigQuery client
   * @param projectId a string containing the current project ID
   * @throws IOException Thrown if there is a network error connecting to
   *                     Bigquery.
   */
  @throws(classOf[IOException])
  def listDatasets(bigquery: Bigquery, projectId: String) {
    val datasetRequest: Bigquery#Datasets#List = bigquery.datasets.list(projectId)
    val datasetList: DatasetList = datasetRequest.execute
    if (datasetList.getDatasets != null) {
      val datasets: List[DatasetList.Datasets] = datasetList.getDatasets
      System.out.println("Available datasets\n----------------")
      System.out.println(datasets.toString)
      import scala.collection.JavaConversions._
      for (dataset <- datasets) {
        System.out.format("%s\n", dataset.getDatasetReference.getDatasetId)
      }
    }
  }
}
