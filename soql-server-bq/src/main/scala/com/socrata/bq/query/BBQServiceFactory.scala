package com.socrata.bq.query

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.http.HttpTransport
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.JsonFactory
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.Bigquery
import com.google.api.services.bigquery.BigqueryScopes
import java.io.IOException
import java.util.Collection

/**
 * This class creates our Service to connect to Bigquery including auth.
 */
object BBQServiceFactory {
  /**
   * Singleton service used through the app.
   */
  private var service: Bigquery = null
  /**
   * Mutex created to create the singleton in thread-safe fashion.
   */
  private val serviceLock: AnyRef = new AnyRef()

  /**
   * Threadsafe Factory that provides an authorized Bigquery service.
   * @return The Bigquery service
   * @throws IOException Thronw if there is an error connecting to Bigquery.
   */
  @throws(classOf[IOException])
  def getService: Bigquery = {
    if (service == null) {
      serviceLock synchronized {
        if (service == null) {
          service = createAuthorizedClient
        }
      }
    }
    service
  }

  /**
   * Creates an authorized client to Google Bigquery.
   * @return The BigQuery Service
   * @throws IOException Thrown if there is an error connecting
   */
  @throws(classOf[IOException])
  private def createAuthorizedClient: Bigquery = {
    val bigqueryScopes: Collection[String] = BigqueryScopes.all
    val transport: HttpTransport = new NetHttpTransport
    val jsonFactory: JsonFactory = new JacksonFactory
    var credential: GoogleCredential = GoogleCredential.getApplicationDefault(transport, jsonFactory)
    if (credential.createScopedRequired) {
      credential = credential.createScoped(bigqueryScopes)
    }
    new Bigquery.Builder(transport, jsonFactory, credential).setApplicationName("BigQuery Samples").build
  }
}
