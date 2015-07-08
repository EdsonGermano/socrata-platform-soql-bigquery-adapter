//package com.socrata.pg.server
//
//import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
//import com.google.api.client.http.javanet.NetHttpTransport
//import com.google.api.client.json.jackson2.JacksonFactory
//import com.google.api.services.bigquery.{Bigquery, BigqueryScopes}
//
//class BigQueryConnection {
//  private val PROJECT_ID = "1093450707280"
//  private val TRANSPORT = new NetHttpTransport()
//  private val JSON_FACTORY = new JacksonFactory()
//
//  val client = {
//    var credential = GoogleCredential.getApplicationDefault()
//    if (credential.createScopedRequired) {
//      credential = credential.createScoped(BigqueryScopes.all)
//    }
//    new Bigquery(TRANSPORT, JSON_FACTORY, credential)
//  }
//}
