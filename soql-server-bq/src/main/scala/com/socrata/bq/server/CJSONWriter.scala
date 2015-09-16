package com.socrata.bq.server

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.io.CompactJsonWriter
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonUtil}
import com.rojoma.simplearm.util._
import com.socrata.bq.query.RowCount
import com.socrata.datacoordinator.common.soql.SoQLRep
import com.socrata.datacoordinator.id.{ColumnId, UserColumnId}
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.util.CloseableIterator
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.types._
import com.socrata.soql.types.obfuscation.CryptProvider
import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import java.io.{OutputStreamWriter, Writer}
import javax.servlet.http.HttpServletResponse

/**
 * Writes rows as CJSON
 *  [
 *    {
 *      approximate_row_count: x,
 *      data_version: y,
 *      last_modified: ISODateTime,
 *      locale: en,
 *      "pk"
 *      schema: [
 *                  {name -> type},
 *                  {name2 -> type}
 *              ],
 *    },
 *    [row 1],
 *    [row 2],
 *    [row 3],
 *
 */
object CJSONWriter {
  val logger: Logger =
    Logger(LoggerFactory getLogger getClass.getName)

  val dateTimeFormat = ISODateTimeFormat.dateTime
  val utf8EncodingName = scala.io.Codec.UTF8.name

  def writeCJson(obfuscationKey: Option[Array[Byte]],
                 qrySchema: OrderedMap[com.socrata.datacoordinator.id.ColumnId, com.socrata.datacoordinator.truth.metadata.ColumnInfo[SoQLType]],
                 rows: CloseableIterator[com.socrata.datacoordinator.Row[SoQLValue]] with RowCount,
                 dataVersion: Long,
                 lastModified: DateTime,
                 locale: String) = (r: HttpServletResponse) => {

    r.setContentType("application/json")
    r.setCharacterEncoding(utf8EncodingName)
    val os = r.getOutputStream

    val cp = new CryptProvider(obfuscationKey.get)
    val jsonReps = SoQLRep.jsonRep(new SoQLID.StringRep(cp), new SoQLVersion.StringRep(cp))

    // Grab the first row in order to get the row count (only accessible after beginning to iterate over
    // the BQ results)
    val firstRow = rows.hasNext match {
      case true => Some(rows.next())
      case false => None
    }
    val rowCount = rows.rowCount
    assert(rowCount.isDefined)

    // Begin writing the JSON
    using(new OutputStreamWriter(os, utf8EncodingName)) { (writer: OutputStreamWriter) =>

      writer.write("[")

      // Grab the schema, JSON reps, and BQ reps
      val cjsonSortedSchema = qrySchema.values.toSeq.sortWith(_.userColumnId.underlying < _.userColumnId.underlying)

      val qryColumnIdToUserColumnIdMap = qrySchema.foldLeft(Map.empty[UserColumnId, ColumnId]) { (map, entry) =>
        val (cid, bbqColInfo) = entry
        map + (bbqColInfo.userColumnId -> cid)
      }
      val reps = cjsonSortedSchema.map { bbqColInfo => jsonReps(bbqColInfo.typ) }.toArray
      val cids = cjsonSortedSchema.map { bbqColInfo => qryColumnIdToUserColumnIdMap(bbqColInfo.userColumnId) }.toArray

      // Write row count, schema, data_version, last_modified, and locale to the JSON
      CompactJsonWriter.toWriter(writer, JObject(Map(
        "approximate_row_count" -> JNumber(rowCount.get),
        "data_version" -> JNumber(dataVersion),
        "last_modified" -> JString(dateTimeFormat.print(lastModified)),
        "locale" -> JString(locale),
        "schema" -> JArray(cjsonSortedSchema.map{colInfo => JObject(Map(
          "c" -> JString(colInfo.userColumnId.underlying),
          "t" -> JString(colInfo.typ.toString())
        ))
        })
      )))

      // Print the rows to the JSON
      // First, iterate over the first row that we had to grab to get the RowCount
      if (firstRow.isDefined) {
        val firstResult = new Array[JValue](firstRow.get.size)
        for (i <- firstResult.indices) {
          firstResult(i) = reps(i).toJValue(firstRow.get(cids(i)))
        }
        writer.write(",\n")
        CompactJsonWriter.toWriter(writer, JArray(firstResult))
      }

      // The rest of the rows
      for (row <- rows) {
        assert(row.size == cids.length)
        val result = new Array[JValue](row.size)

        for (i <- result.indices) {
          result(i) = reps(i).toJValue(row(cids(i)))
        }
        writer.write(",\n")
        CompactJsonWriter.toWriter(writer, JArray(result))
      }

      // End JSON
      writer.write("\n]\n")
      writer.flush()
    }
  }

  private def writeSchema(cjsonSortedSchema: Seq[ColumnInfo[SoQLType]], writer: Writer) {
    val sel = cjsonSortedSchema.map { colInfo => Field(colInfo.userColumnId.underlying, colInfo.typ.toString()) }.toArray
    writer.write("\n ,\"schema\":")
    JsonUtil.writeJson(writer, sel)
  }

  private case class Field(c: String, t: String)

  private implicit val fieldCodec = AutomaticJsonCodecBuilder[Field]

  private type PGJ2CJ = JValue => JValue
}
