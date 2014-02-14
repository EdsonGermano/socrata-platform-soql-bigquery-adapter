package com.socrata.pg.server.config

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import com.typesafe.config.Config

class CuratorConfig(config: Config, root: String) {
  private def k(field: String) = root + "." + field

  val ensemble = config.getStringList(k("ensemble")).asScala.mkString(",")
  val namespace = config.getString(k("namespace"))
  val sessionTimeout = config.getMilliseconds(k("session-timeout")).longValue.millis
  val connectTimeout = config.getMilliseconds(k("connect-timeout")).longValue.millis
  val baseRetryWait = config.getMilliseconds(k("base-retry-wait")).longValue.millis
  val maxRetryWait = config.getMilliseconds(k("max-retry-wait")).longValue.millis
  val maxRetries = config.getInt(k("max-retries"))
}
