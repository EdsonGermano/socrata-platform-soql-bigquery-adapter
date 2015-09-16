package com.socrata.bq.store

import com.typesafe.scalalogging.slf4j.Logging

import scala.concurrent.duration._

import java.nio.charset.StandardCharsets._
import java.security.MessageDigest
import java.util.Comparator

import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.util.Cache
import com.socrata.soql.environment.TypeName

/**
 * Utility for computing a hash for a table schema
 * NOTE: Should match up with the hashing algorithm used by the Soql Postgres Adapter
 *
 * @param typeSerializer grabs the name for the parametrized type
 * @param cache should store already-computed hashes for future requests
 * @tparam CT
 * @tparam CV
 */
class BBQSchemaHasher[CT, CV](typeSerializer: CT => TypeName, cache: Cache) {

  def schemaHash(datasetId: Long,
                 dataVersion: Long,
                 schema: Seq[(UserColumnId, CT)],
                 pk: UserColumnId,
                 locale: String): String = {

    val key = List("schemahash", datasetId.toString, dataVersion.toString)
    cache.lookup[String](key) match {
      case Some(result) =>
        result
      case None =>
        val result = SchemaHash.computeHash(pk, schema, locale, typeSerializer)
        cache.cache(key, result, 24.hours)
        result
    }
  }
}

object SchemaHash extends Logging {
  private val hexDigit = "0123456789abcdef".toCharArray

  private def hexString(xs: Array[Byte]) = {
    val cs = new Array[Char](xs.length * 2)
    var i = 0
    while(i != xs.length) {
      val dst = i << 1
      cs(dst) = hexDigit((xs(i) >> 4) & 0xf)
      cs(dst+1) = hexDigit(xs(i) & 0xf)
      i += 1
    }
    new String(cs)
  }

  // Hashing logic reused from com.socrata.datacoordinator.service.SchemaFinder. Signature changed because we don't
  // have access to ColumnInfo and DatasetCopyContext like PGU does.
  def computeHash[CT](pk: UserColumnId, schema: Seq[(UserColumnId, CT)], locale: String, typeSerializer: CT => TypeName): String = {
    val cols = schema.toArray
    val sha1 = MessageDigest.getInstance("SHA-1")

    sha1.update(locale.getBytes(UTF_8))
    sha1.update(255.toByte)
    sha1.update(pk.underlying.getBytes(UTF_8))
    sha1.update(255.toByte)

    java.util.Arrays.sort(cols, new Comparator[(UserColumnId, CT)] {
      val o = Ordering[UserColumnId]
      def compare(a: (UserColumnId, CT), b: (UserColumnId, CT)) =
        o.compare(a._1, b._1) // compare user column ids of both tuples
    })

    for (col <- cols) {
      val (userColumnId, soqlType) = col
      sha1.update(userColumnId.underlying.getBytes(UTF_8))
      sha1.update(255.toByte)
      sha1.update(typeSerializer(soqlType).caseFolded.getBytes(UTF_8))
      sha1.update(255.toByte)
    }

    hexString(sha1.digest())
  }
}