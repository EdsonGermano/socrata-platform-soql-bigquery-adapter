/*
 * Copyright (c) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not  use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
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
object BigqueryServiceFactory {
  /**
   * Singleton service used through the app.
   */
  private var service: Bigquery = null
  /**
   * Mutex created to create the singleton in thread-safe fashion.
   */
  private var serviceLock: AnyRef = new AnyRef

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
    return service
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
    return new Bigquery.Builder(transport, jsonFactory, credential).setApplicationName("BigQuery Samples").build
  }
}
