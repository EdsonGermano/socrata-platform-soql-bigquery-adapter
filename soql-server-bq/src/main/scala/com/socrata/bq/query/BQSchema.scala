package com.socrata.bq.query

import com.google.api.services.bigquery.model.TableFieldSchema

// Trait to encapsulate the table schema returned from BigQuery
trait BQSchema {
  var tableSchema : List[TableFieldSchema] = null
}

