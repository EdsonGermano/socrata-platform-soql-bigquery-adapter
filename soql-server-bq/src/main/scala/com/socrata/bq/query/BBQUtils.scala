package com.socrata.bq.query

import com.google.api.client.json.GenericJson
import com.google.api.services.bigquery.Bigquery
import com.google.api.services.bigquery.BigqueryRequest
import com.google.api.services.bigquery.model.Job

import java.io.IOException
import java.util.Iterator
import java.util.NoSuchElementException

/**
 * Helper functions for the other classes.
 */
object BBQUtils {

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
}
