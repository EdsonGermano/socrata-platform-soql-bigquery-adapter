package com.socrata.bq.store

object SqlUtils {
  /**
    * Escapes a string appropriately for use with Bigquery. Other less-common
    * control characters could be escaped as well, however the ascii control
    * codes (the \u0001 - \u007f range) caused no issues when tested in simple
    * `WHERE foo = "bar [insert literal control char here]"` queries, so this
    * is probably safe.
    */
  def escapeString(in: String): String = {
    val builder = new StringBuilder()
    for (char <- in) {
      builder.append(char match {
        // The important ones
        case '\\' => "\\\\"
        case '\'' => "\\\'"
        case '\"' => "\\\""
        case '\n' => "\\n"
        // Probably not necessary
        case '\b' => "\\b"
        case '\f' => "\\f"
        case '\r' => "\\r"
        case '\t' => "\\t"
        
        case other => other.toString()
      })
    }
    builder.toString()
  }
}
