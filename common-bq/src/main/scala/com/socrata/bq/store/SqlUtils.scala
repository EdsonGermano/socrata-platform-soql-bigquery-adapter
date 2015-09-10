package com.socrata.bq.store

object SqlUtils {
  /**
    * Escapes a string appropriately for use on the given connection.  Supports
    * postgres wrapped postgres connections that can be discovered via Connection.unwrap.
    */
  def escapeString(in: String): String = {
    val builder = new StringBuilder()
    for (char <- in) {
      builder.append(char match {
        case '\\' => "\\\\"
        case '\'' => "\\\'"
        case '\"' => "\\\""
        case '\b' => "\\b"
        case '\f' => "\\f"
        case '\n' => "\\n"
        case '\r' => "\\r"
        case '\t' => "\\t"
        case c => c.toString()
      })
    }
    builder.toString()
  }
}
